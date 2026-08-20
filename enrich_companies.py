#!/usr/bin/env python3
"""Enrich devb_contractors.csv with Companies Registry data (BRN, address, type, incorp date).

Queries the free CR Open API for each unique English company name, matches the
best result by normalized-name similarity, and writes an enriched copy.

Usage: python enrich_companies.py
"""

import csv
import os
import re
import time
from difflib import SequenceMatcher

import requests

BASE = "https://data.cr.gov.hk/cr/api/api/v1/api_builder/json/local/search"
FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data/processed/Company names, directorships, addresses, public company records, "
    "and lawful procurement data of construction in hong kong",
)
SRC = os.path.join(FOLDER, "devb_contractors.csv")
OUT = os.path.join(FOLDER, "devb_contractors_enriched.csv")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

LEGAL_SUFFIXES = [
    "LIMITED", "LTD", "COMPANY", "CORPORATION", "CORP", "INCORPORATED",
    "INC", "CO", "BERHAD", "SDN BHD", "PTE",
]


def normalize(name: str) -> str:
    """Uppercase, strip punctuation, drop legal suffixes -> core token."""
    s = re.sub(r"[^A-Z0-9]", "", name.upper())
    # strip legal suffixes repeatedly (e.g. "Co Ltd")
    changed = True
    while changed:
        changed = False
        for suffix in LEGAL_SUFFIXES:
            if s.endswith(suffix):
                s = s[: -len(suffix)]
                changed = True
    return s


def _cr_safe_prefix(name: str, n_words: int = 3) -> str:
    """Strip legal suffixes and special chars to make a CR-API-safe
    begins_with prefix. Uses only alphabetic words — the HK CR API
    rejects digits and special characters in this parameter."""
    clean = re.sub(
        r"\b(Limited|Ltd\.?|Co\.?|Company|Corp\.?|Corporation|Hong Kong)\b",
        "", name, flags=re.IGNORECASE,
    )
    # Keep only alphabetic character runs; drop digits, punctuation, CJK.
    words = re.findall(r"[A-Za-z]+", clean)
    return " ".join(words[:n_words])


def search(session: requests.Session, name: str) -> list[dict]:
    # Try 2-word prefix; if that 400s, fall back to 1-word.
    for n in (2, 1):
        prefix = _cr_safe_prefix(name, n)
        if len(prefix) < 2:
            continue
        params = {
            "query[0][key1]": "Comp_name",
            "query[0][key2]": "begins_with",
            "query[0][key3]": prefix,
            "format": "json",
        }
        for attempt in range(3):
            try:
                resp = session.get(BASE, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    return data.get("data", data.get("results", []))
                return []
            except Exception as query_exc:
                if n == 2:
                    break  # 400/error → try 1-word fallback
                if attempt == 2:
                    print(f"    ! query failed for '{name}': {query_exc}")
                    return []
                time.sleep(1.0 * (attempt + 1))
    return []


def best_match(name: str, results: list[dict]) -> tuple[dict | None, float]:
    """Pick the CR record whose normalized name is closest to `name`."""
    target = normalize(name)
    best, best_score = None, 0.0
    for rec in results:
        en = rec.get("English_Company_Name") or rec.get("english_company_name") or ""
        if not en:
            continue
        cand = normalize(en)
        if cand == target:
            return rec, 1.0
        score = SequenceMatcher(None, target, cand).ratio()
        # bonus for prefix containment
        if target and (cand.startswith(target) or target.startswith(cand)):
            score = max(score, 0.9)
        if score > best_score:
            best, best_score = rec, score
    return best, best_score


def main() -> None:
    rows = []
    with open(SRC, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)

    # Unique companies (a company may appear in several category/group rows)
    seen: dict[str, dict] = {}
    for r in rows:
        seen.setdefault(r["name_en"], {})

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    records: dict[str, dict] = {}
    names = list(seen.keys())
    total = len(names)
    matched = 0

    print(f"Enriching {total} unique companies via CR Open API ...")
    for i, name in enumerate(names, 1):
        results = search(session, name)
        rec, score = best_match(name, results)
        if rec and score >= 0.85:
            matched += 1
            records[name] = {
                "brn": rec.get("Brn") or rec.get("brn") or "",
                "registered_address": rec.get("Address_of_Registered_Office")
                or rec.get("address_of_registered_office") or "",
                "company_type": rec.get("Company_Type") or rec.get("company_type") or "",
                "date_of_incorporation": rec.get("Date_of_Incorporation")
                or rec.get("date_of_incorporation") or "",
                "cr_matched_name": rec.get("English_Company_Name")
                or rec.get("english_company_name") or "",
                "cr_match_ratio": round(score, 3),
            }
        if i % 50 == 0:
            print(f"  {i}/{total} done, {matched} matched so far")
        time.sleep(0.4)

    # Build output columns: original + enriched
    orig_cols = list(rows[0].keys())
    new_cols = ["brn", "registered_address", "company_type",
                "date_of_incorporation", "cr_matched_name", "cr_match_ratio"]
    out_cols = orig_cols + new_cols

    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            out = dict(r)
            enrich = records.get(r["name_en"], {})
            for c in new_cols:
                out[c] = enrich.get(c, "")
            w.writerow(out)

    print(f"\nWrote {OUT}")
    print(f"Matched {matched}/{total} unique companies ({(matched/total)*100:.1f}%)")
    print(f"Rows: {len(rows)} (one row per company x category x group)")


if __name__ == "__main__":
    main()
