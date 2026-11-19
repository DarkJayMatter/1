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
                                   "size_band", "isin", "listing_date", "profile_url", "run_id"])
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
                               f.get("profile_url"), self.run_id])
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


JOBS = {"nse": ("R38", run_nse), "careerscan": ("R53", run_careerscan), "erekrut": ("R55", run_erekrut)}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", choices=list(JOBS) + ["all"])
    p.add_argument("--db", help="Postgres URL for the staging database")
    p.add_argument("--csv", help="Directory for CSV output (one file per source)")
    p.add_argument("--delay", type=float, default=1.5, help="Seconds between requests")
    p.add_argument("--start-page", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=3000)
    p.add_argument("--debug", action="store_true", help="Fetch one page, save HTML, print 3 parsed rows")
    args = p.parse_args()
    if not args.db and not args.csv and not args.debug:
        p.error("give --db and/or --csv")
    for name in (JOBS if args.source == "all" else [args.source]):
        sid, fn = JOBS[name]
        sink = Sink(args.db, args.csv, sid)
        try:
            fn(sink, args)
        finally:
            sink.close()


if __name__ == "__main__":
    main()
