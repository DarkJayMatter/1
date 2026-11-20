"""Free, per-company India job evidence from job-board APIs (no scraping of search engines).

Usage (run from account-discovery/):
  python jobsearch.py --input scoring/search_input.csv --out out/jobsearch.csv [--max 300] [--delay 1.2] [--tiers A,B,C]

Providers (each skipped if its key is not set):
  CAREERJET_KEY                    Careerjet v4 publisher API (en_IN), keyword = company name
  JOOBLE_KEY                       Jooble, keyword = company name, location India
  (no key)                         Greenhouse / Lever public boards, guessed slug, India-located jobs only

One company at a time. Resumable (skips keys already in --out). A company is accepted as matched only when the
result's company name passes the name gate. No evidence is recorded as status=no_evidence, never as zero roles.
Stops cleanly on HTTP 429 / quota so the workflow still commits the rows written so far.
Output columns feed scoring/score.py (account_name_key, india_roles_seen, india_role_families).
"""
import argparse, csv, os, re, sys, time
import requests
from account_scrapers import norm, _families

UA = {"User-Agent": "kalvium-account-discovery/1.0 (research; contact via GitHub repo)"}
GENERIC = {"and", "the", "of", "technologies", "technology", "services", "solutions", "systems", "group", "corp",
           "corporation", "company", "co", "global", "software", "consulting", "limited", "ltd", "pvt", "private"}


class Quota(Exception):
    pass


def toks(name):
    t = [w for w in norm(name).split() if w not in GENERIC]
    return t or norm(name).split()


def name_match(a, b):
    """True if one company name's meaningful tokens are all contained in the other's (and not trivially short)."""
    if norm(a) and norm(a) == norm(b):
        return True
    ta, tb = toks(a), toks(b)
    if not ta or not tb:
        return False
    small, big = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    if len("".join(small)) < 4:
        return False
    return all(w in big for w in small)


def _get(url, **kw):
    r = requests.get(url, headers=UA, timeout=30, **kw)
    if r.status_code in (429, 402):
        raise Quota(f"{url.split('?')[0]} -> {r.status_code}")
    return r


def careerjet(name):
    key = os.environ.get("CAREERJET_KEY")
    if not key:
        return None
    r = requests.get("https://search.api.careerjet.net/v4/query", auth=(key, ""), timeout=30, params={
        "locale_code": "en_IN", "keywords": name, "page_size": 50,
        "user_ip": os.environ.get("CAREERJET_USER_IP", "127.0.0.1"), "user_agent": UA["User-Agent"]})
    if r.status_code in (429, 402):
        raise Quota(f"careerjet -> {r.status_code}")
    if r.status_code != 200:
        print(f"careerjet HTTP {r.status_code} for {name!r}: {r.text[:120]}")
        return []
    return [j.get("title", "") for j in r.json().get("jobs", []) if name_match(name, j.get("company", "") or "")]


def jooble(name):
    key = os.environ.get("JOOBLE_KEY")
    if not key:
        return None
    r = requests.post(f"https://jooble.org/api/{key}", json={"keywords": name, "location": "India", "page": 1},
                      headers=UA, timeout=30)
    if r.status_code in (429, 402):
        raise Quota(f"jooble -> {r.status_code}")
    if r.status_code != 200:
        return []
    return [j.get("title", "") for j in r.json().get("jobs", []) if name_match(name, j.get("company", ""))]


def ats(name):
    """Greenhouse / Lever public boards via guessed slug. Only India-located jobs count."""
    base = norm(name)
    slugs = {base.replace(" ", ""), base.replace(" ", "-")}
    out = []
    for s in slugs:
        try:
            r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{s}/jobs", headers=UA, timeout=20)
            if r.status_code == 200:
                out += [j["title"] for j in r.json().get("jobs", [])
                        if "india" in (j.get("location") or {}).get("name", "").lower()]
            r = requests.get(f"https://api.lever.co/v0/postings/{s}?mode=json", headers=UA, timeout=20)
            if r.status_code == 200 and isinstance(r.json(), list):
                out += [j["text"] for j in r.json()
                        if "india" in ((j.get("categories") or {}).get("location") or "").lower()]
        except requests.RequestException:
            continue
    return out


PROVIDERS = [("careerjet", careerjet), ("jooble", jooble), ("ats", ats)]
COLS = ["account_name_key", "name", "tier", "status", "india_roles_seen", "india_role_families", "families",
        "sample_roles", "providers_hit", "providers_tried"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="scoring/search_input.csv")
    ap.add_argument("--out", default="out/jobsearch.csv")
    ap.add_argument("--max", type=int, default=10**9, help="max NEW companies this run (daily cap)")
    ap.add_argument("--delay", type=float, default=1.2)
    ap.add_argument("--tiers", default="A,B,C,D")
    a = ap.parse_args()
    want = set(a.tiers.split(","))
    rows = [r for r in csv.DictReader(open(a.input, encoding="utf-8")) if r["tier"] in want]
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    done = set()
    if os.path.exists(a.out):
        done = {r["account_name_key"] for r in csv.DictReader(open(a.out, encoding="utf-8"))}
    f = open(a.out, "a", newline="", encoding="utf-8")
    w = csv.writer(f)
    if not done:
        w.writerow(COLS)
    print("providers: ats" + (", careerjet" if os.environ.get("CAREERJET_KEY") else "") + (", jooble" if os.environ.get("JOOBLE_KEY") else ""))
    n = 0
    try:
        for r in rows:
            if r["account_name_key"] in done:
                continue
            if n >= a.max:
                break
            titles, hit, tried = [], [], []
            for pname, fn in PROVIDERS:
                res = fn(r["name"])
                if res is None:      # key not set
                    continue
                tried.append(pname)
                if res:
                    hit.append(pname)
                    titles += res
                time.sleep(a.delay)
            titles = list(dict.fromkeys(titles))
            fams = _families(titles)
            status = "ok" if titles else ("no_evidence" if tried else "no_provider")
            w.writerow([r["account_name_key"], r["name"], r["tier"], status, len(titles), len(fams), ",".join(fams),
                        " | ".join(titles[:5]), ",".join(hit), ",".join(tried)])
            f.flush()
            n += 1
            if n % 50 == 0:
                print(f"{n} companies searched")
    except Quota as e:
        print(f"QUOTA reached, stopping cleanly: {e}")
    f.close()
    print(f"jobsearch: {n} new companies -> {a.out}")


if __name__ == "__main__":
    main()
