"""Layer-1 scoring: reads the master CSV, writes ONE scored CSV. No network, no LLM; all rules live in the CSV tables beside this file.
Usage: python score.py ../../Account_Staging_MASTER.csv Account_Staging_MASTER_SCORED.csv [role_evidence.csv]
role_evidence.csv (optional, from stage 2): account_name_key,india_role_families,india_roles_seen
"""
import csv, re, sys, os, collections

HERE = os.path.dirname(os.path.abspath(__file__))
GCC_SRC = {"R63", "R62", "R25"}          # GCC / captive lists
JOBLIST_SRC = {"R54", "R57", "R61", "R59", "R58", "R55"}
TECH_TEAM_SRC = GCC_SRC | {"R8", "R59"}  # GCC, STPI unit, ingrid (product cos hiring software engineers)
key = lambda n: re.sub(r"[^a-z0-9]", "", (n or "").lower())

weights = {r["parameter"]: float(r["weight"]) for r in csv.DictReader(open(f"{HERE}/weights.csv"))}
rules = [(r["business_type"], int(r["score_0_to_5"]), re.compile(r["regex"], re.I))
         for r in csv.DictReader(open(f"{HERE}/business_type_rules.csv"))]
tiers = [(r["tier"], float(r["min_total"]), r["extra_condition"]) for r in csv.DictReader(open(f"{HERE}/tier_thresholds.csv"))]

def classify(row):
    srcs = {row["Source ID"]} | {s for s in row["Also found in"].split(",") if s}
    text = f'{row["Company name (as listed)"]} | {row["Sector / note"]}'.lower()
    label, score = "Unclassified (review)", 2
    for l, s, rx in rules:
        if rx.search(text):
            label, score = l, s; break
    if score > 0:  # overrides never lift a zero (trust, fund, co-op)
        if srcs & GCC_SRC:
            label, score = "GCC / captive centre", 5
        elif "R59" in srcs:
            label, score = "Product company (ingrid)", 5
        elif "R8" in srcs and score < 4:
            label, score = "IT/ITES unit (STPI)", 5
    return label, score, srcs

def size_score(band, srcs, note=""):
    m = re.search(r"([\d,]+)\s*employees", band or "")
    if m:
        n = int(m.group(1).replace(",", ""))
        return (5 if n >= 1200 else 4 if n >= 500 else 3 if n >= 200 else 2 if n >= 50 else 1), f"{n} employees"
    if "layer Upper" in (note or ""):
        return 5, "RBI Upper-layer NBFC (assumed large)"
    if "layer Middle" in (note or ""):
        return 4, "RBI Middle-layer NBFC (assumed mid-large)"
    if srcs & {"R63", "R25"}:
        return 4, "large parent (assumed)"
    return 2, "size unknown"

def hiring_score(srcs):
    n = len(srcs & JOBLIST_SRC)
    return {0: 0, 1: 2, 2: 3, 3: 4}.get(n, 5), n

evidence = {}
if len(sys.argv) > 3 and os.path.exists(sys.argv[3]):
    for r in csv.DictReader(open(sys.argv[3], encoding="utf-8")):
        evidence[r["account_name_key"]] = r

inp, out = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(inp, encoding="utf-8-sig")))
res = []
for r in rows:
    label, biz, srcs = classify(r)
    ev = evidence.get(key(r["Company name (as listed)"]))
    fams = int(ev["india_role_families"]) if ev else 0
    team = 1 if (srcs & TECH_TEAM_SRC) or biz == 5 or fams > 0 else 0
    breadth = min(5, fams + (1 if team and fams == 0 else 0)) if ev else 0
    size, size_note = size_score(r["Headcount band"], srcs, r["Sector / note"])
    hire, njl = hiring_score(srcs)
    india = 1
    total = (biz/5*weights["business_type"] + team*weights["tech_team"] + breadth/5*weights["tech_role_breadth"]
             + size/5*weights["size"] + india*weights["india_presence"] + hire/5*weights["hiring_signal"])
    tech_hire = (biz/5*weights["business_type"] + team*weights["tech_team"] + breadth/5*weights["tech_role_breadth"]
                 + hire/5*weights["hiring_signal"]) / (weights["business_type"]+weights["tech_team"]+weights["tech_role_breadth"]+weights["hiring_signal"]) * 100
    fit = (size/5*weights["size"] + india*weights["india_presence"]) / (weights["size"]+weights["india_presence"]) * 100
    env = {"tech_team": team, "business_type": biz}
    tier = "D"
    for t, mn, cond in ([] if biz <= 1 else tiers):
        if total >= mn and (not cond or eval(re.sub(r"(?<![<>=!])=(?!=)", "==", cond), {"__builtins__": {}}, env)):
            tier = t; break
    reasons = [f"type: {label} ({biz}/5)"]
    if team: reasons.append("tech team: yes" + (" (India roles seen)" if fams else " (source/type)"))
    if njl: reasons.append(f"on {njl} job list(s)")
    reasons.append(size_note)
    level = ("India entity" if srcs & {"R8", "R25", "R62", "R44", "R47", "R48"} else "Brand / parent-level")
    r.update({"Business type": label, "Business type score (0-5)": biz, "Tech team (0/1)": team,
              "Tech role breadth (0-5)": breadth, "Size score (0-5)": size, "Hiring signal (0-5)": hire,
              "Tech-hire score (0-100)": round(tech_hire), "Fit score (0-100)": round(fit), "Total score (0-100)": round(total),
              "Tier": tier, "Entity level": level, "Reasons": "; ".join(reasons[:4]),
              "Needs review": "Y" if label.startswith("Unclassified") else ""})
    res.append(r)

fields = list(rows[0].keys()) + [k for k in res[0] if k not in rows[0]]
fields = list(dict.fromkeys(fields))
res.sort(key=lambda x: (-int(x["Total score (0-100)"]), x["Company name (as listed)"]))
with open(out, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(res)
print("rows", len(res), dict(collections.Counter(x["Tier"] for x in res)))
print("unclassified", sum(1 for x in res if x["Needs review"]))
