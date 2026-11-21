"""Tag stage: adds Vertical, Archetype tags, Tag source, Tag rule, Tag confidence.
Priority: overrides > llm_lookup > source_map (high) > vertical_rules > unclassified.
Usage: python tagging/tag.py out/cleaned.csv out/tagged.csv
"""
import csv, re, sys, os, collections

HERE = os.path.dirname(os.path.abspath(__file__))
PSU = re.compile(r"\b(coal india|ntpc|ongc|gail|bhel|nmdc|power grid|airports authority|bharat (heavy|petroleum|electronics|earth|sanchar)|hindustan (aeronautics|petroleum|copper|zinc)|indian (oil|railway|railways|bank)|state bank of india|life insurance corporation|steel authority)\b", re.I)


def norm(n):
    return "".join(re.findall(r"[a-z0-9]", (n or "").lower()))


def load(path):
    with open(os.path.join(HERE, path), encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def main(src, out):
    rows = list(csv.DictReader(open(src, encoding="utf-8-sig")))
    smap = {r["source_id"]: r for r in load("source_map.csv")}
    rules = [(r["rule_id"], r["vertical"], re.compile(r["pattern"], re.I), r["confidence"]) for r in load("vertical_rules.csv")]
    over = {r["name_norm"]: r for r in load("overrides.csv") if r["name_norm"]}
    llm = {r["name_norm"]: r for r in load("llm_lookup.csv") if r["name_norm"]}
    stats = collections.Counter()
    for r in rows:
        r.update({"Vertical": "", "Archetype tags": "", "Tag source": "", "Tag rule": "", "Tag confidence": ""})
        if r.get("Clean action", "keep") != "keep":
            continue
        name = r.get("Name (clean)") or r["Company name (as listed)"]
        key = norm(name)
        arche = set()
        sources = [r["Source ID"]] + [s.strip().split(" ")[0] for s in re.split(r"[;,]", r.get("Also found in", "")) if s.strip()]
        for s in sources:
            m = smap.get(s)
            if m and m["archetype"]:
                arche.update(a for a in m["archetype"].split(";") if a)
        if PSU.search(name):
            arche.add("PSU")
        vert = src_tag = rule = conf = ""
        if key in over:
            o = over[key]; vert, src_tag, rule, conf = o["vertical"], "override", "override", "high"
            arche.update(a for a in o["archetype"].split(";") if a)
        elif key in llm and llm[key]["vertical"] not in ("", "unknown"):
            o = llm[key]; vert, src_tag, rule, conf = o["vertical"], "llm_lookup", "llm", o["confidence"] or "medium"
            arche.update(a for a in o["archetype"].split(";") if a)
        else:
            m = smap.get(r["Source ID"])
            if m and m["vertical"] and m["confidence"] == "high":
                vert, src_tag, rule, conf = m["vertical"], "source_map", r["Source ID"], "high"
            else:
                text = name + " " + (r.get("Sector / note") or "")
                hits = collections.OrderedDict()
                for rid, v, rx, c in rules:
                    n = len(rx.findall(text))
                    if n:
                        hits[v] = (hits.get(v, (0, rid, c))[0] + n, hits.get(v, (0, rid, c))[1], c)
                if hits:
                    best = max(hits.items(), key=lambda kv: (kv[1][0], kv[1][2] != "low"))
                    vert, src_tag, rule = best[0], "rule", best[1][1]
                    conf = "medium" if best[1][2] == "medium" else "low"
                    if len(hits) > 1 and sorted((v[0] for v in hits.values()), reverse=True)[:2][0] == sorted((v[0] for v in hits.values()), reverse=True)[:2][1]:
                        conf = "low"
        r["Vertical"], r["Tag source"], r["Tag rule"], r["Tag confidence"] = vert, src_tag, rule, conf
        r["Archetype tags"] = "; ".join(sorted(arche))
        stats[(src_tag or "unclassified", conf)] += 1
    fields = list(rows[0].keys())
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)
    print(dict(stats))
    print(collections.Counter(r["Vertical"] for r in rows if r["Vertical"]).most_common())


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
