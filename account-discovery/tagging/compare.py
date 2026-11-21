"""Compare two company tables and tag duplicates.

Tags every row of table B (the "new" table) against table A (the "existing" table):
  Match status : new | duplicate | possible_duplicate
  Match type   : cin | name_exact | name_legal | name_fuzzy_key | domain | name_similar
  Matched ID / Matched name / Match score
Omit table B to dedupe table A against itself (later rows are tagged as duplicates of earlier ones).

Matching reuses clean.py's normalisation. Rules that avoid false merges:
  * names must be location-compatible when both rows have a city/state
  * domain matches count only for non-webmail domains used by <= 3 rows, and need the same first word in the name
  * fuzzy-key and similarity matches need the same first word
  * name_similar (difflib ratio >= --threshold) is only ever "possible_duplicate", for a human to confirm

Usage:
  python tagging/compare.py existing.csv new.csv out.csv [--threshold 0.92] [--both out_a.csv]
  python tagging/compare.py master.csv out.csv --self
"""
import argparse, collections, csv, difflib, os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clean import clean_name, legal_key, fuzzy_key, good_domain  # noqa: E402

NAME_COLS = ["Name (clean)", "Company name (as listed)", "Company name", "Company", "Name", "name"]
ID_COLS = ["Account ID", "ID", "id"]
CIN_COLS = ["CIN", "cin"]
DOM_COLS = ["Domain", "Website", "domain"]
LOC_COLS = ["City / State", "City", "Location", "State"]


def pick(cols, want, options):
    if want:
        if want not in cols:
            sys.exit(f"column '{want}' not found; columns are: {cols}")
        return want
    return next((c for c in options if c in cols), None)


class Table:
    def __init__(self, path, args):
        with open(path, encoding="utf-8-sig", newline="") as f:
            self.rows = list(csv.DictReader(f))
        cols = list(self.rows[0].keys()) if self.rows else []
        self.cols = cols
        self.name = pick(cols, args.name_col, NAME_COLS)
        if not self.name:
            sys.exit(f"{path}: no name column found; use --name-col. Columns: {cols}")
        self.id = pick(cols, None, ID_COLS)
        self.cin = pick(cols, None, CIN_COLS)
        self.dom = pick(cols, None, DOM_COLS)
        self.loc = pick(cols, None, LOC_COLS)

    def prep(self, i, r):
        nm = clean_name(r.get(self.name, ""))
        first = (re.findall(r"[a-z0-9]+", nm.lower()) or [""])[0]
        loc = {t for t in re.findall(r"[a-z]{3,}", (r.get(self.loc, "") if self.loc else "").lower())}
        cin = (r.get(self.cin, "") if self.cin else "").strip().upper()
        return {"i": i, "id": (r.get(self.id) if self.id else None) or f"row{i + 1}", "name": nm, "low": nm.lower(),
                "first": first, "lk": legal_key(nm), "fk": fuzzy_key(nm),
                "dom": good_domain(r.get(self.dom, "") if self.dom else ""), "loc": loc,
                "cin": cin if len(cin) == 21 else ""}


def compatible(a, b):
    return not a or not b or bool(a & b)


class Index:
    def __init__(self, items):
        self.cin, self.exact, self.lk, self.fk = {}, collections.defaultdict(list), collections.defaultdict(list), collections.defaultdict(list)
        self.dom, self.block = collections.defaultdict(list), collections.defaultdict(list)
        self.dom_n = collections.Counter(it["dom"] for it in items if it["dom"])
        for it in items:
            self.add(it)

    def add(self, it):
        if it["cin"]:
            self.cin.setdefault(it["cin"], it)
        self.exact[it["low"]].append(it)
        if len(it["lk"]) >= 5:
            self.lk[it["lk"]].append(it)
        if len(it["fk"]) >= 6:
            self.fk[it["fk"]].append(it)
        if it["dom"]:
            self.dom[it["dom"]].append(it)
        if it["first"]:
            self.block[it["first"]].append(it)

    def find(self, b, threshold):
        if b["cin"] and b["cin"] in self.cin:
            a = self.cin[b["cin"]]; return "duplicate", "cin", a, 1.0
        for a in self.exact.get(b["low"], []):
            if compatible(a["loc"], b["loc"]):
                return "duplicate", "name_exact", a, 1.0
        for a in self.lk.get(b["lk"], []) if len(b["lk"]) >= 5 else []:
            if compatible(a["loc"], b["loc"]):
                return "duplicate", "name_legal", a, 0.98
        for a in self.fk.get(b["fk"], []) if len(b["fk"]) >= 6 else []:
            if a["first"] == b["first"] and compatible(a["loc"], b["loc"]):
                return "duplicate", "name_fuzzy_key", a, 0.95
        if b["dom"] and 1 < self.dom_n[b["dom"]] + 1 <= 4:
            for a in self.dom.get(b["dom"], []):
                if a["first"] == b["first"]:
                    return "duplicate", "domain", a, 0.9
        best, bs = None, 0.0
        for a in self.block.get(b["first"], [])[:400]:
            if not compatible(a["loc"], b["loc"]) or abs(len(a["low"]) - len(b["low"])) > 12:
                continue
            s = difflib.SequenceMatcher(None, a["low"], b["low"]).ratio()
            if s > bs:
                best, bs = a, s
        if best and bs >= threshold:
            return "possible_duplicate", "name_similar", best, round(bs, 3)
        return "new", "", None, 0.0


def tag(table_b, index, threshold, self_mode=False):
    stats = collections.Counter()
    out = []
    for i, r in enumerate(table_b.rows):
        it = table_b.prep(i, r)
        status, mtype, a, score = index.find(it, threshold)
        r = dict(r)
        r["Match status"], r["Match type"] = status, mtype
        r["Matched ID"], r["Matched name"], r["Match score"] = (a["id"], a["name"], score) if a else ("", "", "")
        stats[(status, mtype)] += 1
        out.append(r)
        if self_mode and status == "new":
            index.add(it)
            index.dom_n[it["dom"]] += 1 if it["dom"] else 0
    return out, stats


def write(path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("files", nargs="+", help="A.csv B.csv out.csv   (or with --self: A.csv out.csv)")
    p.add_argument("--self", action="store_true", help="dedupe one table against itself")
    p.add_argument("--name-col", help="name column (applies to both tables)")
    p.add_argument("--threshold", type=float, default=0.92)
    p.add_argument("--both", help="also write table A tagged against B to this path")
    a = p.parse_args()
    if a.self:
        if len(a.files) != 2: sys.exit("--self needs: table.csv out.csv")
        tb = Table(a.files[0], a); out_path = a.files[1]
        idx = Index([]); rows, st = tag(tb, idx, a.threshold, self_mode=True)
    else:
        if len(a.files) != 3: sys.exit("needs: existing.csv new.csv out.csv")
        ta, tb = Table(a.files[0], a), Table(a.files[1], a); out_path = a.files[2]
        idx = Index([ta.prep(i, r) for i, r in enumerate(ta.rows)])
        rows, st = tag(tb, idx, a.threshold)
        if a.both:
            ib = Index([tb.prep(i, r) for i, r in enumerate(tb.rows)])
            ra, _ = tag(ta, ib, a.threshold); write(a.both, ra)
    write(out_path, rows)
    n = len(rows)
    print(f"{n} rows tagged -> {out_path}")
    for (s, t), c in sorted(st.items(), key=lambda kv: -kv[1]):
        print(f"  {s:20s} {t:16s} {c:7d} ({100 * c / n:.1f}%)")


if __name__ == "__main__":
    main()
