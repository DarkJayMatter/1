import re
"""Merge every out/R*.csv (plus the 35 STPI rows already in the sheet) into ONE deduped file."""
import csv, glob, json, os, re, sys

ORDER = ["R87", "R86", "R63", "R62", "R25", "R94", "R26", "R57", "R54", "R59", "R58", "R55", "R61", "R43", "R44", "R47", "R48", "R53", "R80", "R115", "R146", "R139"]
GCC = {"R63", "R62", "R25", "R94"}
URLS = {"R115": "https://trai.gov.in/sites/default/files/2026-07/MIB_23072026.pdf", "R146": "https://apeda.gov.in/sites/default/files/documents/2025-11/Quality_Development_2021_to_2025.pdf", "R139": "https://inc42.com/features/d2c-brands-that-are-disrupting-indias-consumer-market/amp/", "R53": "https://careerscan.in/sitemap.xml",
        "R94": "https://www.scribd.com/",
        "R63": "https://astraviseservices.com/insights/the-india-gcc-index-2026-companies-cities-sectors-and-functions/",
        "R62": "https://gccjournal.in/insights/list-of-global-capability-centers-gcc-in-india/",
        "R54": "https://www.nextknock.in/companies", "R57": "https://hirehire.in/companies.html",
        "R55": "https://www.erekrut.com/all-companies/", "R44": "https://bankzop.com/nbfc-directory/", "R43": "https://www.rbi.org.in/Scripts/BS_NBFCList.aspx",
        "R48": "https://tariffnew.trai.gov.in/service-provider/2", "R47": "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRecognised=yes"}
STPI = ["TTEC India Customer Solutions Pvt. Ltd.", "i-Engineering Software Pvt. Ltd.", "SPEC India", "Effective Teleservices Pvt. Ltd.", "EDAC Solutions Pvt. Ltd.", "EnCircle Exports India Pvt. Ltd.", "Office Beacon Administrative Services Pvt. Ltd.", "Maxim India Integrated Circuit Design Pvt. Ltd.", "Juriscape Legal Research Pvt. Ltd.", "C-Metric Solutions Pvt. Ltd.", "Cybage Software Pvt. Ltd.", "Revitas Technologies Pvt. Ltd.", "Medusind Solutions India Pvt. Ltd.", "StayinFront India Pvt. Ltd.", "S&P Capital IQ India Private Limited", "Tata Consultancy Services Ltd.", "IMEG Engineering (India) Private Limited", "Infor (India) Pvt. Ltd.", "eClinical Works (India) Pvt. Ltd.", "Darshan Soft Tech Pvt. Ltd.", "Investis Digital India Private Limited", "Gowrie Healthcare Pvt. Ltd.", "Hi-Tech iSolutions LLP", "Tax Tech (India) Pvt. Ltd.", "Acty Systems India Pvt. Ltd.", "Infosense Pvt. Ltd.", "Varologic Technologies Pvt. Ltd.", "Print Bear India LLP", "Infosense Services", "AugustCode Solutions Pvt. Ltd.", "Softyoug Solutions", "V360 Technetronic LLP", "IBM India Pvt. Ltd.", "Tech Mahindra Cerium Pvt. Ltd.", "Motorola Solutions Kodiak Networks India Pvt. Ltd."]
key = lambda n: re.sub(r"[^a-z0-9]", "", n.lower())

seen, rows, also, cin_seen = {}, [], {}, {}
def add(name, city, sid, url, note, domain="", band="", date="", status="New", cin=""):
    name = re.sub(r"\s+", " ", name or "").strip()
    k = key(name)
    if not k:
        return
    m = re.search(r"(\d[\d,]*)", band or "")
    if m and "employee" not in (band or "").lower():
        band = f"{m.group(1).replace(',', '')} employees"
    cin = (cin or "").strip().upper()
    if cin and cin in cin_seen:
        k = cin_seen[cin]
    if k in seen:
        also.setdefault(k, set()).add(sid)
        if band and not rows[seen[k]][6]:
            rows[seen[k]][6] = band
        return
    seen[k] = len(rows)
    if cin:
        cin_seen[cin] = k
    rows.append([name, city or "", sid, url or "", date, domain, band, status + (" (GCC)" if sid in GCC else ""), note or "", k, cin])

for n in STPI:
    add(n, "", "R8", "https://stpi.in/en/list-stp-units", "STPI unit", date="2026-11-19", status="New")
for sid in ORDER:
    path = f"out/{sid}.csv"
    if not os.path.exists(path):
        continue
    for x in csv.DictReader(open(path, encoding="utf-8")):
        ex = {}
        try: ex = json.loads(x.get("extra") or "{}")
        except Exception: pass
        d = x["run_id"][:8]; date = f"{d[:4]}-{d[4:6]}-{d[6:]}"
        note = x["industry"] or ""
        if sid == "R44": note = f"NBFC: {note} | {ex.get('cin_or_reg','')}"
        if sid == "R47": note = f"SEBI: {note} | {ex.get('reg_no','')}"
        add(x["name"], x["city"] or x["state"], sid, x["profile_url"] or URLS.get(sid, ""), note,
            domain=ex.get("website") or "", band=x["size_band"] or "", date=date,
            cin=ex.get("cin") or (ex.get("cin_or_reg") if re.match(r"^[LU]\d{5}", ex.get("cin_or_reg") or "") else ""))

out = sys.argv[1] if len(sys.argv) > 1 else "Account_Staging_MASTER.csv"
with open(out, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["Account ID", "Company name (as listed)", "City / State", "Source ID", "Source URL", "Date seen", "CIN", "Domain", "Headcount band", "Status", "Sector / note", "Also found in"])
    for i, r in enumerate(rows, 1):
        w.writerow([f"A{i:05d}", r[0], r[1], r[2], r[3], r[4], r[10], r[5], r[6], r[7], r[8], ",".join(sorted(also.get(r[9], [])))])
from collections import Counter
print(len(rows), dict(Counter(r[2] for r in rows)))
