#!/usr/bin/env python3
"""Build tender_line_and_boq_rate.csv — merge awards/tenders + TPI + material prices."""

import csv
import os
import re
import sqlite3
from collections import defaultdict

PROJ = os.path.dirname(os.path.abspath(__file__))
TPI_PATH = os.path.expanduser("~/Downloads/TPI_RLB_ASD_1970_2025.csv")
RES_PATH = os.path.join(PROJ, "data/processed/hk_construction_resources_2003_2026_cleaned.csv")
DB_PATH = os.path.join(PROJ, "data/procurement/hk_procurement.db")
OUT_PATH = os.path.join(PROJ, "data/processed/tender_line_and_boq_rate.csv")

# --- 1. Load RLB/ASD TPI ---
tpi_data = {}
with open(TPI_PATH) as f:
    for r in csv.DictReader(f):
        tpi_data[(int(r["Year"]), r["Quarter"])] = {
            "tpi_rlb": float(r["RLB"]),
            "tpi_asd": float(r["ASD"]),
        }

# --- 2. Load C&SD material prices ---
mat_by_month = defaultdict(dict)
indicator_cols = []
with open(RES_PATH) as f:
    for r in csv.DictReader(f):
        key = f"{r['indicator']} ({r['unit']})"
        mat_by_month[r["period"]][key] = float(r["value"])
        if key not in indicator_cols:
            indicator_cols.append(key)

print(f"Material indicators: {len(indicator_cols)}")

# --- 3. Load procurement data ---
conn = sqlite3.connect(DB_PATH)
awards = conn.execute("""
    SELECT source, tender_ref, award_date, contractor_name, contract_value,
           contract_value_currency
    FROM awards
    WHERE award_date != '' AND award_date != '-' AND award_date IS NOT NULL
      AND contract_value IS NOT NULL
      AND contractor_name NOT LIKE '%/%'
    ORDER BY award_date
""").fetchall()

tenders = conn.execute("""
    SELECT source, tender_ref, title_en, publication_date, closing_date, status
    FROM tenders ORDER BY closing_date
""").fetchall()
conn.close()

print(f"Awards with value+date: {len(awards)}")
print(f"Tenders: {len(tenders)}")

# --- 4. Build rows ---
MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
          "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def month_to_quarter(m):
    return {1: "Q1", 2: "Q1", 3: "Q1", 4: "Q2", 5: "Q2", 6: "Q2",
            7: "Q3", 8: "Q3", 9: "Q3", 10: "Q4", 11: "Q4", 12: "Q4"}[m]


def parse_period(date_str):
    """Return (year, period, quarter) or (None, None, None)."""
    if not date_str:
        return None, None, None
    # "10 Jul2026" style
    m = re.match(r"(\d{1,2})\s*([A-Za-z]{3})(\d{4})", date_str)
    if m:
        day, mon, yr = int(m.group(1)), MONTHS.get(m.group(2).lower()), int(m.group(3))
        if mon:
            return yr, f"{yr:04d}-{mon:02d}", month_to_quarter(mon)
    # ISO prefix
    if len(date_str) >= 7:
        try:
            y = int(date_str[:4])
            m = int(date_str[5:7])
            return y, f"{y:04d}-{m:02d}", month_to_quarter(m)
        except (ValueError, IndexError):
            pass
    return None, None, None


rows = []

for source, ref, award_date, contractor, value, currency in awards:
    yr, period, quarter = parse_period(award_date)
    if not period:
        continue
    tpi = tpi_data.get((yr, quarter), {})
    mats = mat_by_month.get(period, {})
    row = {
        "record_type": "award",
        "source": source,
        "tender_ref": ref,
        "contractor_name": contractor,
        "award_date": award_date,
        "period": period,
        "quarter": f"{yr} {quarter}",
        "contract_value_hkd": value,
        "currency": currency or "HKD",
        "tpi_rlb": tpi.get("tpi_rlb", ""),
        "tpi_asd": tpi.get("tpi_asd", ""),
    }
    for col in indicator_cols:
        row["mat_" + col] = mats.get(col, "")
    rows.append(row)

for source, ref, title, pub_date, closing_date, status in tenders:
    yr, period, quarter = parse_period(pub_date or closing_date)
    if not period:
        continue
    tpi = tpi_data.get((yr, quarter), {})
    mats = mat_by_month.get(period, {})
    row = {
        "record_type": "tender",
        "source": source,
        "tender_ref": ref or title,
        "contractor_name": "",
        "award_date": "",
        "period": period,
        "quarter": f"{yr} {quarter}",
        "contract_value_hkd": "",
        "currency": "HKD",
        "tpi_rlb": tpi.get("tpi_rlb", ""),
        "tpi_asd": tpi.get("tpi_asd", ""),
    }
    for col in indicator_cols:
        row["mat_" + col] = mats.get(col, "")
    rows.append(row)

# --- 5. Sort & write ---
rows.sort(key=lambda r: r["award_date"] or r["period"])

out_cols = [
    "record_type", "source", "tender_ref", "contractor_name",
    "award_date", "period", "quarter", "contract_value_hkd", "currency",
    "tpi_rlb", "tpi_asd",
] + ["mat_" + c for c in indicator_cols]

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
with open(OUT_PATH, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=out_cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_PATH}")

# --- 6. Stats ---
has_tpi = sum(1 for r in rows if r.get("tpi_rlb") != "")
has_mat = sum(1 for r in rows if any(
    r.get("mat_" + c) not in ("",) for c in indicator_cols))
by_source = defaultdict(int)
for r in rows:
    by_source[r["source"]] += 1
print(f"  Rows with TPI: {has_tpi}/{len(rows)}")
print(f"  Rows with material prices: {has_mat}/{len(rows)}")
print(f"  By source: {dict(by_source)}")
print(f"  Columns: {len(out_cols)}")
print(f"  Material price columns: {len(indicator_cols)}")
if rows:
    first_d = rows[0]["award_date"] or rows[0]["period"]
    last_d = rows[-1]["award_date"] or rows[-1]["period"]
    print(f"  Date range: {first_d} -> {last_d}")