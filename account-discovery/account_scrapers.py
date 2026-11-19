#!/usr/bin/env python3
"""
Bulk scrapers for the big open company lists in the Source Registry.

Sources
  R38  NSE equity list (CSV)            ~2,200 rows
  R53  CareerScan company directory     ~45,000 rows (24 a page)
  R55  Erekrut all-companies directory  ~6,000 rows

Output: Postgres table `staging.companies_raw` (if --db is given) or a CSV per source.
One row per (source_id, name_norm). Re-running is safe: existing rows are skipped.
CIN and domain are NOT filled here; that is the enrich step.

Usage
  pip install requests beautifulsoup4 psycopg[binary]
  python account_scrapers.py nse --csv out/
  python account_scrapers.py careerscan --db postgresql://user:pw@host/db --delay 1.5
  python account_scrapers.py erekrut --db postgresql://... --max-pages 50
  python account_scrapers.py careerscan --debug      # saves page 1 HTML to debug_careerscan.html

NOT TESTED against the live sites (the build environment could not reach them).
The CareerScan and Erekrut selectors are best guesses from rendered text.
Run with --debug first, check the printed sample rows, and adjust the SELECTORS
block below if fields come out empty.
"""
import argparse
import csv
import io
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from urllib import robotparser
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

UA = "KalviumAccountDiscovery/0.1 (contact: enterprise-relations@kalvium.com)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept-Language": "en-IN,en;q=0.8"})

# --- Adjust these if --debug shows empty fields ------------------------------
SELECTORS = {
    "careerscan": {
        "base": "https://careerscan.in/companies",
        "page_param": "page",          # ?page=N (page 2 confirmed to differ from page 1)
        "card_link_prefix": "/companies/",  # profile links look like /companies/<slug>-<id>
    },
    "erekrut": {
        "base": "https://www.erekrut.com/all-companies/",
        "page_path": "page/{n}/",      # /all-companies/page/2/ confirmed to differ from page 1
        "card_link_contains": "/compan",  # GUESS: adjust after looking at debug HTML
    },
}
NSE_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

SCHEMA = """
create schema if not exists staging;
create table if not exists staging.companies_raw (
  id            bigserial primary key,
  source_id     text not null,
  name          text not null,
  name_norm     text not null,
  city          text,
  state         text,
  industry      text,
  size_band     text,
  isin          text,
  listing_date  text,
  profile_url   text,
  raw           jsonb,
  run_id        text,
  first_seen_at timestamptz default now(),
  unique (source_id, name_norm)
);
"""

SUFFIXES = r"\b(private|pvt|limited|ltd|llp|inc|india|\(india\)|the)\b"


def norm(name: str) -> str:
    s = name.lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(SUFFIXES, " ", s)
    return re.sub(r"\s+", " ", s).strip()


# --- polite fetching ---------------------------------------------------------
_robots = {}


def allowed(url: str) -> bool:
    host = re.match(r"https?://[^/]+", url).group(0)
    if host not in _robots:
        rp = robotparser.RobotFileParser()
        try:
            r = SESSION.get(host + "/robots.txt", timeout=20)
            rp.parse(r.text.splitlines() if r.status_code == 200 else [])
        except requests.RequestException:
            rp.parse([])
        _robots[host] = rp
    return _robots[host].can_fetch(UA, url)


def get(url: str, delay: float, retries: int = 4):
    if not allowed(url):
        sys.exit(f"robots.txt disallows {url}. Stop and check the site's terms.")
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45)
            if r.status_code == 200:
                time.sleep(delay)
                return r
            if r.status_code in (403, 429):
                time.sleep(30 * (attempt + 1))  # back off hard; do not hammer
                continue
            if r.status_code == 404:
                return None
        except requests.RequestException:
            pass
        time.sleep(5 * (attempt + 1))
    return None


# --- sinks -------------------------------------------------------------------
class Sink:
    def __init__(self, db_url, csv_dir, source_id):
        self.source_id = source_id
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S-") + uuid.uuid4().hex[:6]
        self.seen = set()
        self.conn = None
        self.csv = None
        if db_url:
            import psycopg
            self.conn = psycopg.connect(db_url, autocommit=True)
            self.conn.execute(SCHEMA)
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)
            path = os.path.join(csv_dir, f"{source_id}.csv")
            new = not os.path.exists(path)
            self.csv_f = open(path, "a", newline="", encoding="utf-8")
            self.csv = csv.writer(self.csv_f)
            if new:
                self.csv.writerow(["source_id", "name", "name_norm", "city", "state", "industry",
                                   "size_band", "isin", "listing_date", "profile_url", "run_id", "extra"])
            else:  # load names already written so a re-run does not duplicate
                with open(path, encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        self.seen.add(row["name_norm"])
        self.added = 0

    def add(self, name, **f):
        n = norm(name)
        if not n or n in self.seen:
            return False
        self.seen.add(n)
        if self.conn:
            cur = self.conn.execute(
                """insert into staging.companies_raw
                   (source_id,name,name_norm,city,state,industry,size_band,isin,listing_date,profile_url,raw,run_id)
                   values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   on conflict (source_id,name_norm) do nothing""",
                (self.source_id, name, n, f.get("city"), f.get("state"), f.get("industry"),
                 f.get("size_band"), f.get("isin"), f.get("listing_date"), f.get("profile_url"),
                 json.dumps(f.get("raw") or {}), self.run_id))
            if cur.rowcount == 0:
                return False
        if self.csv:
            self.csv.writerow([self.source_id, name, n, f.get("city"), f.get("state"), f.get("industry"),
                               f.get("size_band"), f.get("isin"), f.get("listing_date"),
                               f.get("profile_url"), self.run_id, json.dumps(f.get("raw") or {}, ensure_ascii=False)])
        self.added += 1
        return True

    def close(self):
        if self.csv:
            self.csv_f.close()
        if self.conn:
            self.conn.close()


# --- R38: NSE ----------------------------------------------------------------
def run_nse(sink, args):
    r = get(NSE_URL, args.delay)
    if r is None:
        sys.exit("Could not download EQUITY_L.csv (NSE sometimes needs a browser-like header).")
    reader = csv.DictReader(io.StringIO(r.text))
    for row in reader:
        row = {k.strip(): (v or "").strip() for k, v in row.items()}
        sink.add(row["NAME OF COMPANY"], isin=row.get("ISIN NUMBER"),
                 listing_date=row.get("DATE OF LISTING"),
                 raw={"symbol": row.get("SYMBOL"), "series": row.get("SERIES")})
    print(f"NSE: {sink.added} new rows")


# --- R53: CareerScan ---------------------------------------------------------
SIZE_RE = re.compile(r"(\d[\d,]*\s*[-–]\s*\d[\d,]*|\d[\d,]*\+)\s*employees", re.I)
LOC_RE = re.compile(r"([A-Za-z .&]+),\s*India")


def parse_careerscan(html):
    soup = BeautifulSoup(html, "html.parser")
    cfg = SELECTORS["careerscan"]
    rows, seen_urls = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        path = href.replace("https://careerscan.in", "")
        if not path.startswith(cfg["card_link_prefix"]) or path.rstrip("/") == cfg["card_link_prefix"].rstrip("/"):
            continue
        url = urljoin(cfg["base"], href)
        if url in seen_urls:
            continue
        card = a.find_parent(["article", "li", "div"])
        h = card.find(["h2", "h3"]) if card else None
        if not h:
            continue
        seen_urls.add(url)
        text = card.get_text(" | ", strip=True)
        size = SIZE_RE.search(text)
        loc = LOC_RE.search(text)
        rows.append({"name": h.get_text(strip=True), "profile_url": url,
                     "state": loc.group(1).strip() if loc else None,
                     "size_band": size.group(1).replace(" ", "") if size else None,
                     "raw": {"card_text": text[:400]}})
    return rows


def run_careerscan(sink, args):
    cfg = SELECTORS["careerscan"]
    page, empty_streak = args.start_page, 0
    while page <= args.max_pages:
        r = get(f"{cfg['base']}?{cfg['page_param']}={page}", args.delay)
        if r is None:
            print(f"page {page}: no response, stopping (resume with --start-page {page})")
            break
        if args.debug and page == args.start_page:
            open("debug_careerscan.html", "w", encoding="utf-8").write(r.text)
        rows = parse_careerscan(r.text)
        new = sum(sink.add(x.pop("name"), **x) for x in rows)
        print(f"page {page}: {len(rows)} cards, {new} new")
        if args.debug:
            print(json.dumps(rows[:3], indent=1))
            return
        empty_streak = empty_streak + 1 if not rows else 0
        if empty_streak >= 2:
            break
        page += 1
    print(f"CareerScan: {sink.added} new rows (last page {page})")


# --- R55: Erekrut ------------------------------------------------------------
def parse_erekrut(html):
    """Cards are <a href=/company/slug><h5>Name</h5></a> followed by a location span."""
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()
    for a in soup.find_all("a", href=True):
        if "/company/" not in a["href"]:
            continue
        h = a.find(["h5", "h4", "h3"])
        if not h:  # skips the "Visit Jobs" links
            continue
        name = h.get_text(" ", strip=True)
        if not name or a["href"] in seen:
            continue
        seen.add(a["href"])
        box = a.parent
        loc = box.find("span") if box else None
        place = loc.get_text(" ", strip=True) if loc else ""
        state = re.sub(r",?\s*IN$", "", place).strip() or None
        raw = {"location": place}
        if name.endswith(".."):
            # site truncates long names; rebuild from the profile slug
            raw["listed_name"] = name
            slug = a["href"].rstrip("/").rsplit("/", 1)[-1]
            name = " ".join(w.capitalize() for w in slug.split("-"))
        rows.append({"name": name, "profile_url": a["href"], "state": state,
                     "raw": raw})
    return rows


def run_erekrut(sink, args):
    cfg = SELECTORS["erekrut"]
    page, empty_streak, stale_streak = args.start_page, 0, 0
    while page <= args.max_pages:
        url = f'{cfg["base"]}?company_name=&indstry_name=&farea_name=&selcity=&selstate=&pages={page}'
        r = get(url, args.delay)
        if r is None:
            print(f"page {page}: no response, stopping (resume with --start-page {page})")
            break
        if args.debug and page == args.start_page:
            open("debug_erekrut.html", "w", encoding="utf-8").write(r.text)
        rows = parse_erekrut(r.text)
        new = sum(sink.add(x.pop("name"), **x) for x in rows)
        print(f"page {page}: {len(rows)} cards, {new} new")
        if args.debug:
            print(json.dumps(rows[:3], indent=1))
            return
        empty_streak = empty_streak + 1 if not rows else 0
        stale_streak = stale_streak + 1 if new == 0 else 0
        if empty_streak >= 2 or stale_streak >= 3:
            break
        page += 1
    print(f"Erekrut: {sink.added} new rows (last page {page})")


def parse_tables(html):
    """Generic: every <table> -> rows; name = first column matching company/parent/name, else col 0."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for t in soup.find_all("table"):
        heads = [c.get_text(" ", strip=True).lower() for c in (t.find("tr").find_all(["th", "td"]) if t.find("tr") else [])]
        prev = t.find_previous(["strong", "b", "h2", "h3", "h4"])
        sector = prev.get_text(" ", strip=True) if prev else None
        def col(*keys):
            for i, h in enumerate(heads):
                if any(k in h for k in keys):
                    return i
            return None
        ni = col("parent", "company", "entity", "name")
        if ni is None:  # not a company table (e.g. stats)
            continue
        ci = col("city", "cities", "location")
        for tr in t.find_all("tr")[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) <= ni or not cells[ni]:
                continue
            link = tr.find("a", href=True)
            out.append({"name": cells[ni], "city": cells[ci] if ci is not None and ci < len(cells) else None,
                        "industry": sector, "profile_url": link["href"] if link else None,
                        "raw": dict(zip(heads, cells))})
    return out


def make_table_runner(label, url):
    def run(sink, args):
        r = get(url, args.delay)
        if r is None:
            print(f"{label}: no response"); return
        if args.debug:
            open(f"debug_{label}.html", "w", encoding="utf-8").write(r.text)
        rows = parse_tables(r.text)
        new = sum(sink.add(x.pop("name"), **x) for x in rows)
        print(f"{label}: {len(rows)} rows, {new} new")
        if args.debug:
            print(json.dumps(rows[:3], indent=1))
    return run


def _sec(url, delay):
    """SEC fair access: declared User-Agent, <=10 req/s. APIs are public; no robots check."""
    for attempt in range(4):
        try:
            r = SESSION.get(url, timeout=45)
            if r.status_code == 200:
                time.sleep(delay)
                return r
            if r.status_code in (403, 429):
                time.sleep(5 * (attempt + 1))
            else:
                return None
        except requests.RequestException:
            time.sleep(2)
    return None


def run_edgar(sink, args):
    """Parents from out/R63.csv -> latest 10-K -> Exhibit 21 -> subsidiaries in India."""
    parents = [row["name"] for row in csv.DictReader(open("out/R63.csv", encoding="utf-8"))]
    # split multi-parent cells like "FedEx, Lufthansa"
    parents = sorted({x.strip() for p in parents for x in re.split(r",\s*(?![^()]*\))", p) if x.strip()})
    r = _sec("https://www.sec.gov/files/company_tickers.json", 0.2)
    if r is None:
        print("EDGAR: tickers file unavailable"); return
    tickers = [(norm(v["title"]), v["cik_str"], v["title"]) for v in r.json().values()]
    for parent in parents:
        pn = norm(parent)
        cands = [t for t in tickers if t[0] == pn or t[0].startswith(pn + " ")]
        if not cands:
            print(f"- {parent}: no SEC match (probably non-US)"); continue
        _, cik, title = min(cands, key=lambda t: len(t[0]))
        sub = _sec(f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json", args.delay)
        if sub is None:
            print(f"- {parent}: submissions failed"); continue
        rec = sub.json()["filings"]["recent"]
        ten_k = [i for i, f in enumerate(rec["form"]) if f == "10-K"]
        if not ten_k:
            print(f"- {parent} ({title}): no 10-K"); continue
        i = ten_k[0]
        acc = rec["accessionNumber"][i].replace("-", "")
        idx = _sec(f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/index.json", args.delay)
        if idx is None:
            print(f"- {parent}: filing index failed"); continue
        files = [it["name"] for it in idx.json()["directory"]["item"]]
        ex = [f for f in files if re.search(r"(ex|exhibit)[-_ ]?0?21", f, re.I)]
        if not ex:
            print(f"- {parent} ({title}): no Ex-21 file"); continue
        doc = _sec(f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{ex[0]}", args.delay)
        if doc is None:
            print(f"- {parent}: Ex-21 download failed"); continue
        soup = BeautifulSoup(doc.text, "html.parser")
        names = set()
        for tr in soup.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"]) if c.get_text(strip=True)]
            if cells and any(c.lower() == "india" or c.lower().endswith(", india") for c in cells[1:]):
                names.add(cells[0])
            elif cells and "india" in cells[0].lower():
                names.add(cells[0])
        if not names:  # plain-text exhibits
            for line in soup.get_text("\n").splitlines():
                if "india" in line.lower() and 3 < len(line.strip()) < 150:
                    names.add(line.strip())
        added = 0
        for nm in names:
            added += sink.add(nm, industry=f"subsidiary of {parent}", profile_url=f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{ex[0]}",
                              raw={"parent": parent, "sec_title": title, "filed": rec["filingDate"][i]})
        print(f"+ {parent} ({title}, 10-K {rec['filingDate'][i]}): {len(names)} India entities, {added} new")
    print(f"EDGAR: {sink.added} new rows")


PROBES = {
    "sebi_9": "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRecognisedFpi=yes&intmId=9",
    "sebi_13": "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRecognisedFpi=yes&intmId=13",
    "sebi_23": "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRecognisedFpi=yes&intmId=23",
}


def run_probe(sink, args):
    """Save raw HTML of first page of each source so parsers can be written against real markup."""
    os.makedirs("out/probe", exist_ok=True)
    for k, url in PROBES.items():
        try:
            if not allowed(url):
                print(f"{k}: robots.txt disallows"); continue
            r = SESSION.get(url, timeout=45)
            open(f"out/probe/{k}.html", "w", encoding="utf-8").write(r.text)
            print(f"{k}: HTTP {r.status_code}, {len(r.text)} bytes")
        except Exception as e:
            print(f"{k}: failed {e}")
        time.sleep(args.delay)


def run_nextknock(sink, args):
    """One page lists all companies: <a href=/companies/slug>Name<span>open roles</span></a>."""
    r = get("https://www.nextknock.in/companies", args.delay)
    if r is None:
        print("nextknock: no response"); return
    soup = BeautifulSoup(r.text, "html.parser")
    n = new = 0
    for a in soup.find_all("a", href=re.compile(r"^/companies/[^/]+$")):
        span = a.find("span")
        roles = span.get_text(strip=True) if span else None
        if span:
            span.extract()
        name = a.get_text(" ", strip=True)
        if not name:
            continue
        n += 1
        new += sink.add(name, profile_url="https://www.nextknock.in" + a["href"], raw={"open_roles": roles})
    print(f"nextknock: {n} companies, {new} new")


def run_hirehire(sink, args):
    r = get("https://hirehire.in/companies.html", args.delay)
    if r is None:
        print("hirehire: no response"); return
    soup = BeautifulSoup(r.text, "html.parser")
    n = new = 0
    for a in soup.select("a.ci-card"):
        b = a.find("b"); sp = a.find("span")
        if not b:
            continue
        info = sp.get_text(" ", strip=True) if sp else ""
        sector = info.split("·")[-1].strip() if "·" in info else None
        n += 1
        new += sink.add(b.get_text(strip=True), industry=sector,
                        profile_url="https://hirehire.in" + a["href"], raw={"info": info})
    print(f"hirehire: {n} companies, {new} new")


def run_trai(sink, args):
    r = get("https://tariffnew.trai.gov.in/service-provider/2", args.delay)
    if r is None:
        print("trai: no response"); return
    rows = parse_tables(r.text)
    new = sum(sink.add(x.pop("name"), industry="ISP (TRAI)", raw=x["raw"]) for x in rows)
    print(f"trai: {len(rows)} rows, {new} new")


def run_bankzop(sink, args):
    """BankZop NBFC directory via the JSON endpoint its own page calls (20 per page)."""
    page = args.start_page
    while page <= args.max_pages:
        url = f"https://www.bankzop.com/api/nbfc/search?page={page}&per_page=20&q=a"
        if not allowed(url):
            print("bankzop: robots.txt disallows the API; stopping"); return
        try:
            r = SESSION.get(url, headers={"Accept": "application/json"}, timeout=45)
            d = r.json()
        except Exception as e:
            print(f"bankzop page {page}: failed ({e}); stopping"); return
        rows = d.get("data") or []
        new = sum(sink.add(x["name"], state=x.get("state"), industry=x.get("category"),
                           raw={"cin_or_reg": x.get("registration_number")}) for x in rows)
        print(f"page {page}/{d.get('pages')}: {len(rows)} rows, {new} new")
        if not rows or page >= (d.get("pages") or 0):
            break
        page += 1
        time.sleep(args.delay)


def run_careerjet(sink, args):
    """/companies/{letter}[/{page}] -> <a href=/company/Slug/jobs>Name</a>; stop a letter when a page adds nothing."""
    base = "https://www.careerjet.co.in/companies/"
    for letter in list("abcdefghijklmnopqrstuvwxyz") + ["_"]:
        stale = 0
        for page in range(1, args.max_pages + 1):
            url = base + letter + ("" if page == 1 else f"/{page}")
            r = get(url, args.delay)
            if r is None:
                print(f"{letter} p{page}: no response, next letter"); break
            soup = BeautifulSoup(r.text, "html.parser")
            links = [a for a in soup.find_all("a", href=re.compile(r"^/company/[^/]+/jobs$")) if a.get_text(strip=True)]
            new = sum(sink.add(a.get_text(" ", strip=True), profile_url="https://www.careerjet.co.in" + a["href"]) for a in links)
            print(f"{letter} p{page}: {len(links)} links, {new} new")
            if not links:
                break
            stale = stale + 1 if new == 0 else 0
            if stale >= 2:
                break
    print(f"Careerjet: {sink.added} new rows")


JOBS = {"nse": ("R38", run_nse), "careerscan": ("R53", run_careerscan), "erekrut": ("R55", run_erekrut),
        "edgar": ("R25", run_edgar),
        "probe": ("PROBE", run_probe),
        "careerjet": ("R61", run_careerjet),
        "nextknock": ("R54", run_nextknock),
        "hirehire": ("R57", run_hirehire),
        "trai": ("R48", run_trai),
        "bankzop": ("R44", run_bankzop),
        "astravise": ("R63", make_table_runner("astravise", "https://astraviseservices.com/insights/the-india-gcc-index-2026-companies-cities-sectors-and-functions/")),
        "gccjournal": ("R62", make_table_runner("gccjournal", "https://gccjournal.in/insights/list-of-global-capability-centers-gcc-in-india/"))}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", help="one of: " + ", ".join(JOBS) + ", all, or a comma-separated list")
    p.add_argument("--db", help="Postgres URL for the staging database")
    p.add_argument("--csv", help="Directory for CSV output (one file per source)")
    p.add_argument("--delay", type=float, default=1.5, help="Seconds between requests")
    p.add_argument("--start-page", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=3000)
    p.add_argument("--debug", action="store_true", help="Fetch one page, save HTML, print 3 parsed rows")
    args = p.parse_args()
    if not args.db and not args.csv and not args.debug:
        p.error("give --db and/or --csv")
    names = list(JOBS) if args.source == "all" else args.source.split(",")
    bad = [n for n in names if n not in JOBS]
    if bad:
        p.error(f"unknown source: {bad}")
    for name in names:
        sid, fn = JOBS[name]
        sink = Sink(args.db, args.csv, sid)
        try:
            fn(sink, args)
        finally:
            sink.close()


if __name__ == "__main__":
    main()
