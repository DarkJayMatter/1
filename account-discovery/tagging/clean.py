"""Stage 1 (clean): master CSV -> cleaned master CSV. No network, no LLM. Rows are never deleted: each gets Clean action keep / exclude / duplicate.
Usage: python tagging/clean.py out/master_v10.csv out/cleaned.csv
Adds: Name (clean), Clean action, Clean reason, Duplicate of
"""
import csv, html, re, sys, collections

SMALL = {"and", "of", "the", "for", "in", "at", "to", "ltd", "pvt"}
FIX = {"Llp": "LLP", "Pvt": "Pvt", "Ltd": "Ltd", "Opc": "OPC", "India": "India", "It": "IT", "Hr": "HR", "Ai": "AI", "Usa": "USA", "Uk": "UK"}
LEGAL = r"(pvt\.?|private|ltd\.?|limited|llp|inc\.?|incorporated|corp\.?|corporation|co\.?|company|opc|plc|llc|gmbh|\(india\)|india)"
GENERIC = {"technologies", "technology", "solutions", "services", "systems", "software", "global", "group", "international", "infotech", "tech", "labs", "digital",
           "consulting", "consultants", "enterprises", "industries", "ventures", "works", "network", "networks", "and", "the", "of"}
FRAG = re.compile(r"^(private|pvt|ltd|limited|llp|lim|pri|consultan\w*|infotech|advisors|computers|companies|company|india|group|services|solutions)$", re.I)
PERSON = re.compile(r"^(mr|mrs|ms|miss|smt)\b\.?\s", re.I)
COMPANY_WORDS = re.compile(r"\b(pvt|private|ltd|limited|llp|technolog\w*|solutions|services|enterprises|industries|exports?|traders?|agro|finance|capital|bank|securities|"
                           r"investments?|systems|software|labs?|mechanic\w*|motors|foods?)\b", re.I)
NONTARGET = re.compile(r"\bsociety\b|co-?operative|sahakari|\bhuf\b|proprietor|\bsamiti\b|\bmandal\b|charitable|gratuity|provident fund|"
                       r"\bmutual fund\b|\breit\b|\binvit\b|alternative investment fund|\baif\b", re.I)
TRUST = re.compile(r"\btrust\b", re.I)
FREEMAIL = ("gmail", "yahoo", "hotmail", "rediff", "outlook", "ymail", "live.", "aol.")


def clean_name(n):
    n = html.unescape(n or "")
    n = re.sub(r"\s+", " ", n).strip()
    n = re.sub(r"^\d{1,3}\s*[.)]\s+", "", n)            # leading "12. "
    n = re.sub(r"[\s,;:\-–|/.]+$", "", n)               # trailing punctuation
    n = re.sub(r"\s+,", ",", n)
    letters = [c for c in n if c.isalpha()]
    if letters and sum(c.isupper() for c in letters) / len(letters) > 0.85 and len(n) > 5:   # ALL CAPS -> Title Case
        n = " ".join(w if (w.lower() in SMALL and i) else (w.capitalize() if len(w) > 3 or not w.isupper() or w.lower() in ("ltd", "pvt") else w) for i, w in enumerate(n.split()))
        n = " ".join(FIX.get(w, w) for w in n.split())
    return n


def fuzzy_key(n):
    s = n.lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\b" + LEGAL + r"\b", " ", s)
    toks = [t for t in s.split() if t not in GENERIC]
    return "".join(toks)


def loc_tokens(r):
    return {t for t in re.findall(r"[a-z]{3,}", (r.get("City / State") or "").lower())}


def compatible(a, b):
    return not a or not b or bool(a & b)


def legal_key(n):
    s = re.sub(r"[^a-z0-9 ]", " ", n.lower())
    s = re.sub(r"\b" + LEGAL + r"\b", " ", s)
    return "".join(s.split())


def good_domain(d):
    d = (d or "").lower().replace("www.", "").split(";")[0].strip()
    return d if "." in d and not any(f in d for f in FREEMAIL) else ""


def main(inp, out):
    rows = list(csv.DictReader(open(inp, encoding="utf-8-sig")))
    dom_count = collections.Counter(good_domain(r.get("Domain")) for r in rows)
    seen_legal, seen_fuzzy, seen_dom = {}, {}, {}
    stats = collections.Counter()
    for r in rows:
        raw = r["Company name (as listed)"]
        nm = clean_name(raw)
        r["Name (clean)"] = nm
        r["Clean action"], r["Clean reason"], r["Duplicate of"] = "keep", "", ""
        letters = sum(c.isalpha() for c in nm)
        reason = None
        if letters < 3 or len(nm) < 3:
            reason = "junk_name: too short or no letters"
        elif FRAG.match(nm) or re.match(r"^jobs\b", nm, re.I):
            reason = "junk_name: generic fragment"
        elif PERSON.match(nm) and len(nm.split()) <= 4 and not COMPANY_WORDS.search(nm):
            reason = "person_name: individual, not a company"
        elif NONTARGET.search(nm) or (TRUST.search(nm) and not COMPANY_WORDS.search(nm)):
            reason = "non_target_entity: trust, society, co-op, fund or proprietorship"
        if reason:
            r["Clean action"], r["Clean reason"] = "exclude", reason
            stats[reason.split(":")[0]] += 1
            continue
        lk, fk, dom, lt = legal_key(nm), fuzzy_key(nm), good_domain(r.get("Domain")), loc_tokens(r)
        hit = None
        first = (re.findall(r"[a-z0-9]+", nm.lower()) or [""])[0]
        if dom and 1 < dom_count[dom] <= 3 and dom in seen_dom and seen_dom[dom][1] == first:
            hit = (seen_dom[dom][0], "same domain and same first word")
        if not hit and len(lk) >= 5:
            for aid, alt in seen_legal.get(lk, []):
                if compatible(lt, alt):
                    hit = (aid, "same name after removing legal suffixes"); break
        if not hit and len(fk) >= 6:
            for aid, alt, f0 in seen_fuzzy.get(fk, []):
                if f0 == first and compatible(lt, alt):
                    hit = (aid, "same name after removing legal suffixes and generic words"); break
        if hit:
            r["Clean action"], r["Clean reason"], r["Duplicate of"] = "duplicate", hit[1], hit[0]
            stats["duplicate"] += 1
        else:
            if len(lk) >= 5:
                seen_legal.setdefault(lk, []).append((r["Account ID"], lt))
            if len(fk) >= 6:
                seen_fuzzy.setdefault(fk, []).append((r["Account ID"], lt, first))
            if dom:
                seen_dom.setdefault(dom, (r["Account ID"], first))
    fields = list(rows[0].keys())
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)
    print("rows", len(rows), dict(stats), "keep", sum(1 for r in rows if r["Clean action"] == "keep"))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
