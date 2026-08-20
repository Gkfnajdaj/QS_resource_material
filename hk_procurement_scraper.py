#!/usr/bin/env python3
"""Headless scraper for HK construction procurement data.

Scrapes tender notices, contract awards, approved contractor lists, and
company registry profiles from four HK government sources into a shared
SQLite database. Supports a foreground ``--watch`` loop for continuous,
incremental ingestion (default daily).

Sources:
- DEVB — Approved Contractors for Public Works (JS data files)
- Housing Authority — Tender notices + contract awards (JS-rendered SPA)
- GLD eGazette — Government tender notices
- Companies Registry Open API — Company profiles (name, address, BRN)
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import random
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DB_PATH = Path("./data/procurement/hk_procurement.db")
MANIFEST_PATH = Path("./data/procurement_manifest.json")
RAW_DIR = Path("./data/raw")
PROCESSED_DIR = Path("./data/procurement")

# Source URLs
DEVB_CONTRACTOR_PAGE = (
    "https://www.devb.gov.hk/en/construction_sector_matters/"
    "contractors/contractor/index.html"
)
DEVB_JS_BASE = "https://www.devb.gov.hk/filemanager/externaldb/"

HA_TENDER_URL = (
    "https://www.housingauthority.gov.hk/en/business-partnerships/tenders/index.html"
)
HA_AWARD_URL = (
    "https://www.housingauthority.gov.hk/en/commercial-properties/"
    "tender-notices-and-awards/index.html"
)

# eGazette — the landing page is a Vue.js SPA with search form.
# Government Notices (incl. public tenders) live behind category filter:
#   https://egazette.gld.gov.hk/en/search-gazette/gazette?c=1
# Full scraping requires Playwright form-fill interaction (future enhancement).
GLD_EGAZETTE_URL = "https://egazette.gld.gov.hk/en/search-gazette/gazette?c=1"

CR_API_BASE = "https://data.cr.gov.hk/cr/api/api/v1/api_builder/json/local/search"
CR_API_MIN_QUERY = 2

# Contract-award archives (construction works departments + GLD supply contracts).
# These are plain-HTTP sources (no JS/CAPTCHA) — the deepest reachable histories:
#   ArchSD 2021→present, HyD/CEDD 6–12 months, GLD CSV rolling 12 months.
ARCHSD_AWARDS_URL = (
    "https://www.archsd.gov.hk/en/tenders-notices/works/"
    "contracts-awarded-in-the-past-three-months.html"
)
GLD_AWARDS_CSV_URL = "https://www.gld.gov.hk/datagovhk/procurement/ContractsAwarded_EN.csv"
HYD_AWARDS_URL = "https://www.hyd.gov.hk/en/tender_notices/contracts/awarded/index.html"
CEDD_AWARDS_URL = "https://www.cedd.gov.hk/eng/tender-notices/contracts/contracts-awarded/index.html"
CEDD_DETAIL_BASE = "https://www.cedd.gov.hk/eng/tender-notices/contracts/contracts-awarded/"

REQUEST_TIMEOUT = 60
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

log = logging.getLogger("hk_procurement_scraper")


# --------------------------------------------------------------------------- #
# Networking helpers (reused from hk_csd_scraper.py)
# --------------------------------------------------------------------------- #

def _retry_with_backoff(
    fn: Callable,
    *args,
    retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,),
    **kwargs,
):
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except exceptions as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == retries - 1:
                break
            delay = min(max_delay, base_delay * (2 ** attempt)) + random.uniform(0, 0.5)
            log.warning(
                "Attempt %d/%d failed (%s); retrying in %.1fs",
                attempt + 1, retries, exc, delay,
            )
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


def _http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Playwright render
# --------------------------------------------------------------------------- #

def _render_page_with_playwright(
    url: str,
    dump_path: Optional[Path] = None,
    wait_selector: Optional[str] = None,
    timeout: int = 45000,
) -> str:
    """Render a JS-heavy page with Playwright; always required (no fallback)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error(
            "Playwright is not installed. Run: "
            "`pip install playwright && playwright install chromium`"
        )
        raise

    def _render():
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(user_agent=USER_AGENT)
                try:
                    page.goto(url, wait_until="networkidle", timeout=timeout)
                except Exception:  # noqa: BLE001
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                if wait_selector:
                    try:
                        page.wait_for_selector(
                            wait_selector, timeout=min(timeout, 15000)
                        )
                    except Exception:  # noqa: BLE001
                        log.debug("Selector '%s' did not appear; proceeding anyway",
                                  wait_selector)
                else:
                    page.wait_for_timeout(4000)
                return page.content()
            finally:
                browser.close()

    html = _retry_with_backoff(_render, retries=3)

    if dump_path is not None:
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        dump_path.write_text(html, encoding="utf-8")
        log.info("Saved rendered HTML (%d chars) to %s", len(html), dump_path)
    log.info("Rendered page (%d chars)", len(html))
    return html


# --------------------------------------------------------------------------- #
# Manifest (idempotency)
# --------------------------------------------------------------------------- #

def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            log.warning("Corrupt manifest at %s; starting fresh", MANIFEST_PATH)
    return {"version": 1, "files": {}}


def _save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )


# --------------------------------------------------------------------------- #
# Database layer
# --------------------------------------------------------------------------- #

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    brn TEXT PRIMARY KEY,
    english_name TEXT,
    chinese_name TEXT,
    registered_address TEXT,
    company_type TEXT,
    date_of_incorporation TEXT,
    source TEXT DEFAULT 'cr_api',
    first_seen TEXT,
    last_updated TEXT
);

CREATE TABLE IF NOT EXISTS devb_contractors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name_en TEXT NOT NULL,
    name_zh TEXT,
    category TEXT,
    group_code TEXT,
    status TEXT,
    source_url TEXT,
    first_seen TEXT,
    last_updated TEXT,
    UNIQUE(name_en, category, group_code)
);

CREATE TABLE IF NOT EXISTS tenders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    tender_ref TEXT,
    title_en TEXT,
    procurement_category TEXT,
    issuing_dept TEXT,
    publication_date TEXT,
    closing_date TEXT,
    status TEXT DEFAULT 'open',
    estimated_value REAL,
    tender_url TEXT,
    raw_html TEXT,
    first_seen TEXT,
    last_updated TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tenders_dedup
    ON tenders(source, tender_ref, title_en, closing_date)
    WHERE tender_ref != '' AND title_en != '';

CREATE TABLE IF NOT EXISTS awards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tender_ref TEXT,
    award_date TEXT,
    contractor_name TEXT,
    contractor_brn TEXT,
    contract_value REAL,
    contract_value_currency TEXT DEFAULT 'HKD',
    source TEXT,
    source_url TEXT,
    first_seen TEXT,
    FOREIGN KEY (contractor_brn) REFERENCES companies(brn)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_awards_dedup
    ON awards(source, tender_ref, contractor_name, award_date)
    WHERE tender_ref != '' OR contractor_name != '';
"""


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def _upsert_devb_contractor(
    conn: sqlite3.Connection,
    name_en: str,
    name_zh: str,
    category: str,
    group_code: str,
    status: str,
    source_url: str,
) -> bool:
    now = _now_iso()
    cur = conn.execute(
        """INSERT INTO devb_contractors
           (name_en, name_zh, category, group_code, status, source_url, first_seen, last_updated)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(name_en, category, group_code) DO UPDATE SET
             name_zh=excluded.name_zh,
             status=excluded.status,
             source_url=excluded.source_url,
             last_updated=excluded.last_updated""",
        (name_en, name_zh, category, group_code, status, source_url, now, now),
    )
    return cur.rowcount > 0


def _upsert_tender(
    conn: sqlite3.Connection,
    source: str,
    tender_ref: str,
    title_en: str,
    procurement_category: str = "",
    issuing_dept: str = "",
    publication_date: str = "",
    closing_date: str = "",
    status: str = "open",
    estimated_value: Optional[float] = None,
    tender_url: str = "",
    raw_html: str = "",
) -> bool:
    now = _now_iso()
    try:
        conn.execute(
            """INSERT INTO tenders
               (source, tender_ref, title_en, procurement_category, issuing_dept,
                publication_date, closing_date, status, estimated_value, tender_url,
                raw_html, first_seen, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tender_url) DO UPDATE SET
                 status=excluded.status,
                 closing_date=excluded.closing_date,
                 last_updated=excluded.last_updated""",
            (source, tender_ref, title_en, procurement_category, issuing_dept,
             publication_date, closing_date, status, estimated_value, tender_url,
             raw_html, now, now),
        )
        return True
    except Exception:
        return False


def _upsert_award(
    conn: sqlite3.Connection,
    tender_ref: str,
    award_date: str,
    contractor_name: str,
    contract_value: Optional[float] = None,
    contract_value_currency: str = "HKD",
    source: str = "",
    source_url: str = "",
) -> bool:
    now = _now_iso()
    try:
        conn.execute(
            """INSERT INTO awards
               (tender_ref, award_date, contractor_name, contract_value,
                contract_value_currency, source, source_url, first_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(source_url) DO UPDATE SET
                 contract_value=excluded.contract_value,
                 award_date=excluded.award_date,
                 contractor_name=excluded.contractor_name""",
            (tender_ref, award_date, contractor_name, contract_value,
             contract_value_currency, source, source_url, now),
        )
        return True
    except Exception:
        return False


def _upsert_company(
    conn: sqlite3.Connection,
    brn: str,
    english_name: str = "",
    chinese_name: str = "",
    registered_address: str = "",
    company_type: str = "",
    date_of_incorporation: str = "",
) -> bool:
    now = _now_iso()
    try:
        conn.execute(
            """INSERT INTO companies
               (brn, english_name, chinese_name, registered_address, company_type,
                date_of_incorporation, first_seen, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(brn) DO UPDATE SET
                 english_name=excluded.english_name,
                 chinese_name=excluded.chinese_name,
                 registered_address=excluded.registered_address,
                 last_updated=excluded.last_updated""",
            (brn, english_name, chinese_name, registered_address, company_type,
             date_of_incorporation, now, now),
        )
        return True
    except Exception:
        return False


def _get_unlinked_awards(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    rows = conn.execute(
        """SELECT id, contractor_name FROM awards
           WHERE contractor_brn IS NULL AND contractor_name IS NOT NULL
           AND contractor_name != ''"""
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _link_award_brn(conn: sqlite3.Connection, award_id: int, brn: str) -> None:
    conn.execute(
        "UPDATE awards SET contractor_brn = ? WHERE id = ?", (brn, award_id)
    )


def _export_csvs(conn: sqlite3.Connection, output_dir: Path) -> None:
    import pandas as pd

    output_dir.mkdir(parents=True, exist_ok=True)
    for table in ["devb_contractors", "tenders", "awards", "companies"]:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        path = output_dir / f"{table}.csv"
        df.to_csv(path, index=False)
        log.info("Exported %s: %d rows → %s", table, len(df), path)


# --------------------------------------------------------------------------- #
# Unified award/tender compiler
# --------------------------------------------------------------------------- #

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_date_iso(value: str) -> str:
    """Normalize a loose date string to ISO ``YYYY-MM-DD`` (empty on failure)."""
    if not value or not isinstance(value, str):
        return ""
    v = value.strip().replace("<br/>", " ").replace("\n", " ")
    v = re.sub(r"\s+", " ", v)

    # "14 Aug2026" / "28 Feb2027"
    m = re.match(r"^(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})$", v)
    if m:
        day, mon, year = int(m.group(1)), _MONTHS.get(m.group(2).lower()), int(m.group(3))
        if mon and 1 <= day <= 31:
            return f"{year:04d}-{mon:02d}-{day:02d}"

    # "27-Jun-25" / "7-Mar-25" (GLD CSV style, 2-digit year)
    m = re.match(r"^(\d{1,2})[-/]([A-Za-z]{3})[-/](\d{2})$", v)
    if m:
        day, mon, yy = int(m.group(1)), _MONTHS.get(m.group(2).lower()), int(m.group(3))
        year = 2000 + yy if yy < 70 else 1900 + yy
        if mon and 1 <= day <= 31:
            return f"{year:04d}-{mon:02d}-{day:02d}"

    # "DD-MM-YYYY" / "DD/MM/YYYY"
    m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$", v)
    if m:
        day, mon, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mon <= 12 and 1 <= day <= 31:
            return f"{year:04d}-{mon:02d}-{day:02d}"

    # Already ISO "YYYY-MM-DD"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return v

    # "August 2026" → first of month
    m = re.match(r"^([A-Za-z]+)\s+(\d{4})$", v)
    if m:
        mon = _MONTHS.get(m.group(1).lower())
        if mon:
            return f"{int(m.group(2)):04d}-{mon:02d}-01"

    return ""


def _clean_money(value) -> float:
    """Parse a HK dollar amount to float, stripping commas/currency symbols."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    s = s.replace(",", "").replace("HK$", "").replace("$", "").strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _nan_to_str(value) -> str:
    """Convert NaN/None to '' and strip; else return the stripped string."""
    if value is None:
        return ""
    if isinstance(value, float) and value != value:  # NaN
        return ""
    return str(value).strip()


def compile_unified_awards(conn: sqlite3.Connection, output_dir: Path) -> Path:
    """Merge tenders + awards into one cleaned, date-sorted CSV."""
    import pandas as pd

    tenders = pd.read_sql_query(
        "SELECT source, tender_ref, title_en, procurement_category, issuing_dept, "
        "publication_date, closing_date, status, estimated_value, tender_url "
        "FROM tenders", conn,
    )
    awards = pd.read_sql_query(
        "SELECT tender_ref, award_date, contractor_name, contractor_brn, "
        "contract_value, contract_value_currency, source, source_url "
        "FROM awards", conn,
    )

    rows: list[dict] = []

    for _, t in tenders.iterrows():
        pub = _parse_date_iso(t["publication_date"] or "")
        close = _parse_date_iso(t["closing_date"] or "")
        rows.append({
            "record_type": "tender",
            "source": t["source"],
            "tender_ref": (t["tender_ref"] or "").strip(),
            "title": (t["title_en"] or "").strip(),
            "contractor_name": "",
            "contractor_brn": "",
            "contract_value": None,
            "currency": "HKD",
            "status": t["status"] or "open",
            "award_date": "",
            "publication_date": pub,
            "closing_date": close,
            "sort_date": pub or close,
            "url": t["tender_url"] or "",
        })

    for _, a in awards.iterrows():
        # Drop continuation rows produced by HA rowspan tables
        cname = (a["contractor_name"] or "").strip()
        if cname.endswith("/") or cname == "":
            continue
        award = _parse_date_iso(a["award_date"] or "")
        value = _clean_money(a["contract_value"])
        # Drop rows with neither a date nor a value (e.g. HA shop-rent listings)
        if not award and value is None:
            continue
        rows.append({
            "record_type": "award",
            "source": a["source"],
            "tender_ref": (a["tender_ref"] or "").strip(),
            "title": "",
            "contractor_name": cname,
            "contractor_brn": _nan_to_str(a.get("contractor_brn")),
            "contract_value": value,
            "currency": a["contract_value_currency"] or "HKD",
            "status": "awarded",
            "award_date": award,
            "publication_date": "",
            "closing_date": "",
            "sort_date": award,
            "url": a["source_url"] or "",
        })

    df = pd.DataFrame(rows, columns=[
        "sort_date", "record_type", "source", "tender_ref", "title",
        "contractor_name", "contractor_brn", "contract_value", "currency",
        "status", "award_date", "publication_date", "closing_date", "url",
    ])
    df = df.sort_values(by=["sort_date", "record_type"], kind="mergesort",
                        na_position="last").reset_index(drop=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "hk_contract_awards_unified.csv"
    df.drop(columns=["sort_date"]).to_csv(path, index=False)
    log.info("Compiled %d records → %s", len(df), path)
    return path




# --------------------------------------------------------------------------- #
# CrApiClient — Companies Registry Open API
# --------------------------------------------------------------------------- #

class CrApiClient:
    """Query the free HK Companies Registry Open API for company profiles."""

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or _http_session()

    def search_by_name(self, name_prefix: str) -> list[dict]:
        if len(name_prefix.strip()) < CR_API_MIN_QUERY:
            log.debug("CR API: skipping '%s' (< %d chars)", name_prefix, CR_API_MIN_QUERY)
            return []

        params = {
            "query[0][key1]": "Comp_name",
            "query[0][key2]": "begins_with",
            "query[0][key3]": name_prefix.strip(),
            "format": "json",
        }

        def _fetch():
            resp = self.session.get(
                CR_API_BASE, params=params, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            return resp.json()

        try:
            result = _retry_with_backoff(_fetch, retries=3)
        except Exception as exc:
            log.error("CR API: query failed for '%s': %s", name_prefix, exc)
            return []

        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return result.get("data", result.get("results", []))
        return []

    def lookup_contractor(self, name: str) -> Optional[str]:
        # Skip entries that clearly aren't company names
        # (numeric rent values, location/trade descriptions with slashes, etc.)
        stripped = name.strip()
        if not stripped:
            return None
        # Skip if starts with a number + comma (rent value like "4,600")
        if re.match(r'^\d[\d,]*\s*[/,]', stripped):
            return None
        # Skip if it has a slash AND doesn't look like a company
        if '/' in stripped and not re.search(
            r'\b(limited|ltd|company|corp|corporation|co\.|ltd\.|construction|engineering|building)\b',
            stripped, re.IGNORECASE,
        ):
            return None

        clean = re.sub(
            r'\b(Limited|Ltd\.?|Co\.?|Company|Corp\.?|Corporation|Hong Kong)\b',
            '', name, flags=re.IGNORECASE,
        ).strip().strip(',').strip()

        if len(clean) < CR_API_MIN_QUERY:
            clean = name.strip()

        results = self.search_by_name(clean[:50])
        if results:
            best = results[0]
            return best.get("Brn") or best.get("brn")
        return None


# --------------------------------------------------------------------------- #
# DevbScraper — DEVB Approved Contractors
# --------------------------------------------------------------------------- #

class DevbScraper:
    """Scrape DEVB Approved Contractors for Public Works from JS data files."""

    def __init__(self, conn: sqlite3.Connection, session: Optional[requests.Session] = None):
        self.conn = conn
        self.session = session or _http_session()

    def discover_js_files(self) -> list[str]:
        resp = _retry_with_backoff(
            self.session.get, DEVB_CONTRACTOR_PAGE, timeout=REQUEST_TIMEOUT
        )
        html = resp.text
        links: set[str] = set()
        for m in re.finditer(r'list_contractor_\d+\.js', html):
            links.add(urljoin(DEVB_JS_BASE, m.group(0)))
        soup = BeautifulSoup(html, "lxml")
        for script in soup.find_all("script", src=True):
            src = script["src"]
            if "list_contractor_" in src:
                links.add(urljoin(DEVB_JS_BASE, src))
        return sorted(links)

    def parse_js_file(self, url: str) -> list[dict]:
        resp = _retry_with_backoff(
            self.session.get, url, timeout=REQUEST_TIMEOUT
        )
        text = resp.text
        records: list[dict] = []

        m = re.search(r'=\s*(\[[\s\S]*?\])\s*;?\s*$', text)
        if not m:
            m = re.search(r'(\[[\s\S]*?\])', text)
        if not m:
            return records

        raw = m.group(1)
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass

        # Regex fallback for JS object literals
        parts = re.split(r'\},\s*\{', raw.strip('[] \t\n\r'))
        for part in parts:
            rec: dict = {}
            for field in ["name_en", "name_zh", "category", "group", "status"]:
                fm = re.search(
                    rf'{field}["\']?\s*:\s*["\']([^"\']*)["\']',
                    part, re.IGNORECASE,
                )
                if fm:
                    rec[field] = fm.group(1)
            # Also try numeric/boolean values
            for field in ["status_code", "suspension"]:
                fm = re.search(rf'{field}["\']?\s*:\s*([^,\}}]+)', part, re.IGNORECASE)
                if fm:
                    val = fm.group(1).strip().strip('"\'')
                    rec[field] = val
            if rec:
                records.append(rec)
        return records

    def scrape(self) -> int:
        """Discover JS files, parse each, pivot category columns into rows."""
        CATEGORY_MAP = {
            "buildings": "Buildings",
            "port": "Port Works",
            "road": "Roads & Drainage",
            "site": "Site Formation",
            "water": "Waterworks",
        }

        js_urls = self.discover_js_files()
        log.info("DEVB: discovered %d JS data file(s)", len(js_urls))
        count = 0
        for url in js_urls:
            try:
                records = self.parse_js_file(url)
                for rec in records:
                    name_en = rec.get("e_name", "")
                    name_zh = rec.get("c_name", "") or rec.get("s_name", "")
                    if not name_en:
                        continue
                    for key, category in CATEGORY_MAP.items():
                        val = rec.get(f"e_{key}", "-")
                        if not val or val == "-":
                            continue
                        # Extract group code: "CP" → C (probationary), "A" → A, "BP" → B
                        clean_val = re.sub(r"<[^>]+>", "", val).strip()
                        group_code = clean_val[0] if clean_val else ""
                        # Parse suspension status from HTML
                        status = "active"
                        if "suspended" in val.lower() or "取消投標資格" in val:
                            status = "suspended"

                        _upsert_devb_contractor(
                            self.conn,
                            name_en=name_en,
                            name_zh=name_zh,
                            category=category,
                            group_code=group_code,
                            status=status,
                            source_url=url,
                        )
                        count += 1
                log.info("DEVB: upserted %d records from %s", len(records), url)
            except Exception as exc:
                log.error("DEVB: failed to process %s: %s", url, exc)
        self.conn.commit()
        return count


# --------------------------------------------------------------------------- #
# HaScraper — Housing Authority Tenders + Awards
# --------------------------------------------------------------------------- #

class HaScraper:
    """Scrape Housing Authority tender notices and contract awards."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def scrape_tenders(self) -> int:
        log.info("HA: scraping tenders from %s", HA_TENDER_URL)
        dump_dir = RAW_DIR / "procurement_ha"
        html = _render_page_with_playwright(
            HA_TENDER_URL,
            dump_path=dump_dir / "tenders_rendered.html",
            wait_selector="table, [class*=tender]",
        )
        soup = BeautifulSoup(html, "lxml")
        count = 0

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            col_map: dict[str, int] = {}
            for row in rows:
                cells = row.find_all(["td", "th"])
                texts = [c.get_text(strip=True) for c in cells]
                if not col_map:
                    for idx, h in enumerate(texts):
                        h_low = h.lower()
                        if "reference" in h_low:
                            col_map["ref"] = idx
                        elif "description" in h_low or "title" in h_low:
                            col_map["title"] = idx
                        elif "issue" in h_low or "publication" in h_low:
                            col_map["issue_date"] = idx
                        elif "closing" in h_low:
                            col_map["closing_date"] = idx
                    continue  # skip header row

                tender_ref = texts[col_map.get("ref", 0)] if col_map.get("ref") is not None and len(texts) > col_map.get("ref", 0) else ""
                title_en = texts[col_map.get("title", 1)] if col_map.get("title") is not None and len(texts) > col_map.get("title", 1) else ""
                issue_date = texts[col_map.get("issue_date", 2)] if col_map.get("issue_date") is not None and len(texts) > col_map.get("issue_date", 2) else ""
                closing_date = texts[col_map.get("closing_date", 3)] if col_map.get("closing_date") is not None and len(texts) > col_map.get("closing_date", 3) else ""

                if not title_en:
                    continue

                anchor = row.find("a", href=True)
                href = anchor["href"] if anchor else ""
                tender_url = urljoin(HA_TENDER_URL, href) if href else HA_TENDER_URL

                try:
                    cur = self.conn.execute(
                        """INSERT OR IGNORE INTO tenders
                           (source, tender_ref, title_en, publication_date, closing_date,
                            tender_url, raw_html, first_seen, last_updated)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        ("ha", tender_ref, title_en, issue_date, closing_date,
                         tender_url, str(row), _now_iso(), _now_iso()),
                    )
                    if cur.rowcount > 0:
                        count += 1
                except Exception:
                    pass

        self.conn.commit()
        log.info("HA: upserted %d tenders", count)
        return count

    def scrape_awards(self) -> int:
        log.info("HA: scraping awards from %s", HA_AWARD_URL)
        dump_dir = RAW_DIR / "procurement_ha"
        html = _render_page_with_playwright(
            HA_AWARD_URL,
            dump_path=dump_dir / "awards_rendered.html",
            wait_selector="table, [class*=award]",
        )
        soup = BeautifulSoup(html, "lxml")
        count = 0

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            col_map: dict[str, int] = {}
            for row in rows:
                cells = row.find_all(["td", "th"])
                texts = [c.get_text(strip=True) for c in cells]
                if len(texts) < 2:
                    continue
                if not col_map:
                    for idx, h in enumerate(texts):
                        h_low = h.lower()
                        if "estate" in h_low or "district" in h_low:
                            col_map["estate"] = idx
                        elif "shop" in h_low or "no" in h_low:
                            col_map["shop_no"] = idx
                        elif "area" in h_low:
                            col_map["area"] = idx
                        elif "rent" in h_low or "price" in h_low:
                            col_map["rent"] = idx
                        elif "trade" in h_low or "business" in h_low:
                            col_map["trade"] = idx
                        elif "closing" in h_low or "date" in h_low:
                            col_map["closing_date"] = idx
                    # If no header detected, use positional columns
                    if not col_map:
                        col_map = {"estate": 1, "shop_no": 2, "area": 3, "trade": 5, "rent": 6, "closing_date": 7}
                    continue

                # Data row: extract using column map
                def _get(idx_key: str) -> str:
                    idx = col_map.get(idx_key, -1)
                    if 0 <= idx < len(texts):
                        return texts[idx]
                    return ""

                estate = _get("estate")
                shop_no = _get("shop_no")
                trade_type = _get("trade")
                closing_date = _get("closing_date")
                rent_str = _get("rent")

                contract_value = None
                if rent_str:
                    try:
                        # Skip date-format strings (e.g. "28/08/2026")
                        if not re.match(r'^\d{1,2}/\d{1,2}/\d{2,4}$', rent_str):
                            contract_value = float(
                                re.sub(r'[^\d.]', '', rent_str.replace(',', ''))
                            )
                    except ValueError:
                        pass

                if not estate and not shop_no:
                    continue

                try:
                    cur = self.conn.execute(
                        """INSERT OR IGNORE INTO awards
                           (tender_ref, award_date, contractor_name, contract_value,
                            source, source_url, first_seen)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (shop_no, closing_date, f"{estate} / {trade_type}",
                         contract_value, "ha", HA_AWARD_URL, _now_iso()),
                    )
                    if cur.rowcount > 0:
                        count += 1
                except Exception:
                    pass

        self.conn.commit()
        log.info("HA: upserted %d awards", count)
        return count

    def scrape_all(self) -> tuple[int, int]:
        return self.scrape_tenders(), self.scrape_awards()


# --------------------------------------------------------------------------- #
# GldGazetteScraper — GLD eGazette Tender Notices
# --------------------------------------------------------------------------- #

class GldGazetteScraper:
    """Scrape GLD eGazette tender notices."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def scrape(self) -> int:
        log.info("GLD: scraping eGazette government notices from %s", GLD_EGAZETTE_URL)
        dump_dir = RAW_DIR / "procurement_gld"
        html = _render_page_with_playwright(
            GLD_EGAZETTE_URL,
            dump_path=dump_dir / "egazette_rendered.html",
            wait_selector="table, [class*=result], [class*=notice]",
            timeout=60000,
        )
        soup = BeautifulSoup(html, "lxml")
        count = 0
        tables = soup.find_all("table")
        rows: list = []
        for table in tables:
            rows.extend(table.find_all("tr"))
        if not rows:
            rows = soup.select("[class*=notice], [class*=item], [class*=result], tr, li")

        for row in rows:
            cells = row.find_all(["td", "th"]) if row.name in ("tr", None) else [row]
            if len(cells) < 2:
                continue
            texts = [c.get_text(strip=True) for c in cells]

            tender_ref = texts[0] if len(texts) > 0 else ""
            title_en = texts[1] if len(texts) > 1 else ""
            closing_date = texts[2] if len(texts) > 2 else ""

            anchor = row.find("a", href=True)
            href = anchor["href"] if anchor else ""
            tender_url = urljoin(GLD_EGAZETTE_URL, href) if href else GLD_EGAZETTE_URL

            if not title_en and not tender_ref:
                continue

            try:
                cur = self.conn.execute(
                    """INSERT OR IGNORE INTO tenders
                       (source, tender_ref, title_en, closing_date,
                        tender_url, raw_html, first_seen, last_updated)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    ("gld_egazette", tender_ref, title_en, closing_date,
                     tender_url, str(row), _now_iso(), _now_iso()),
                )
                if cur.rowcount > 0:
                    count += 1
            except Exception:
                pass

        self.conn.commit()
        if count == 0:
            log.warning(
                "GLD: no tender notices extracted. The eGazette is a Vue.js SPA — "
                "tender lists may require form-fill interaction. "
                "See saved HTML at %s for inspection.",
                dump_dir / "egazette_rendered.html",
            )
        else:
            log.info("GLD: upserted %d tender notices", count)
        return count


# --------------------------------------------------------------------------- #
# Contract-award archives (works departments + GLD)
# --------------------------------------------------------------------------- #

def _html_table_rows(html: str) -> list[list[str]]:
    """Flatten every table in an HTML doc into a list of cell-text rows."""
    soup = BeautifulSoup(html, "lxml")
    out: list[list[str]] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if any(cells):
                out.append(cells)
    return out


def _header_map(header: list[str], wanted: dict[str, list[str]]) -> dict[str, int]:
    """Map wanted field -> column index by matching header cells against keywords."""
    col_map: dict[str, int] = {}
    for idx, h in enumerate(header):
        h_low = h.lower()
        for field, keys in wanted.items():
            if field in col_map:
                continue
            if any(k in h_low for k in keys):
                col_map[field] = idx
    return col_map


def _award_insert(
    conn: sqlite3.Connection, tender_ref: str, award_date: str,
    contractor_name: str, contract_value, source: str, source_url: str,
) -> int:
    """Upsert one award row; returns 1 if newly inserted, else 0."""
    if not contractor_name or contractor_name.strip() in ("", "-"):
        return 0
    if isinstance(contract_value, str):
        contract_value = _clean_money(contract_value)
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO awards
               (tender_ref, award_date, contractor_name, contract_value,
                contract_value_currency, source, source_url, first_seen)
               VALUES (?, ?, ?, ?, 'HKD', ?, ?, ?)""",
            (tender_ref, award_date, contractor_name, contract_value,
             source, source_url, _now_iso()),
        )
        return 1 if cur.rowcount > 0 else 0
    except Exception:
        return 0


class ArchsdAwardsScraper:
    """ArchSD works contracts awarded (2021→present, inline JSON, no JS needed)."""

    def __init__(self, conn: sqlite3.Connection, session: requests.Session):
        self.conn = conn
        self.session = session

    @staticmethod
    def _parse_json(html: str) -> list[dict]:
        start = html.index("contractsAwardedJSON")
        eq = html.index("=", start)
        lb = html.index("[", eq)
        depth, end = 0, None
        for i in range(lb, len(html)):
            if html[i] == "[":
                depth += 1
            elif html[i] == "]":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        raw = html[lb:end + 1]
        fields = ["tenderReference", "tenderingProcedure", "particulars",
                  "awardDate", "removalDate", "contractorName",
                  "contractorAddress", "amount"]
        values = {}
        for f in fields:
            pat = re.compile(f + r'\s*:\s*"((?:[^"\\]|\\.)*)"', re.S)
            values[f] = [x.replace('\\"', '"').replace("\\n", " ")
                         for x in pat.findall(raw)]
        n = len(values["tenderReference"])
        return [
            {f: (values[f][i] if i < len(values[f]) else "") for f in fields}
            for i in range(n)
        ]

    def scrape(self) -> int:
        log.info("ArchSD: scraping awards from %s", ARCHSD_AWARDS_URL)
        resp = _retry_with_backoff(
            self.session.get, ARCHSD_AWARDS_URL, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        records = self._parse_json(resp.text)
        count = 0
        for r in records:
            value = _clean_money(r.get("amount", ""))
            inserted = _award_insert(
                self.conn,
                r.get("tenderReference", ""),
                r.get("awardDate", ""),
                r.get("contractorName", ""),
                value,
                "archsd",
                ARCHSD_AWARDS_URL,
            )
            count += inserted
        self.conn.commit()
        log.info("ArchSD: upserted %d awards (%d parsed)", count, len(records))
        return count


class GldCsvAwardsScraper:
    """GLD supply contracts awarded — rolling 12-month CSV on data.gov.hk."""

    def __init__(self, conn: sqlite3.Connection, session: requests.Session):
        self.conn = conn
        self.session = session

    def scrape(self) -> int:
        log.info("GLD: scraping awards CSV from %s", GLD_AWARDS_CSV_URL)
        resp = _retry_with_backoff(
            self.session.get, GLD_AWARDS_CSV_URL, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))
        if not rows:
            return 0
        header = [h.strip().lower() for h in rows[0]]
        col = {h: i for i, h in enumerate(header)}

        def field(r, *names):
            for n in names:
                if n in col and col[n] < len(r):
                    return r[col[n]].strip()
            return ""

        count = 0
        for r in rows[1:]:
            if len(r) < 2:
                continue
            tender_ref = field(r, "tender reference", "tender_ref")
            contractor = field(r, "contractor(s)", "contractor", "contractors")
            amount = field(r, "amount")
            award_date = _parse_date_iso(field(r, "contract award date", "award date"))
            if not contractor:
                continue
            count += _award_insert(
                self.conn, tender_ref, award_date, contractor,
                amount, "gld", GLD_AWARDS_CSV_URL,
            )
        self.conn.commit()
        log.info("GLD: upserted %d awards", count)
        return count


class HydAwardsScraper:
    """Highways Department contracts awarded (rolling 12 months, plain HTML)."""

    def __init__(self, conn: sqlite3.Connection, session: requests.Session):
        self.conn = conn
        self.session = session

    def scrape(self) -> int:
        log.info("HyD: scraping awards from %s", HYD_AWARDS_URL)
        resp = _retry_with_backoff(
            self.session.get, HYD_AWARDS_URL, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        rows = _html_table_rows(resp.text)
        count = 0
        for row in rows:
            joined = " ".join(row).lower()
            if "contract no." in joined and "contractor name" in joined:
                continue  # header row
            if len(row) < 5:
                continue
            tender_ref = row[0].strip()
            award_date = _parse_date_iso(row[2])
            contractor = row[3].strip()
            # Contract Sum ($M) — store in millions as raw HK$ (×1,000,000)
            value = _clean_money(row[5]) if len(row) > 5 else None
            if value is not None:
                value = value * 1_000_000
            count += _award_insert(
                self.conn, tender_ref, award_date, contractor, value,
                "hyd", HYD_AWARDS_URL,
            )
        self.conn.commit()
        log.info("HyD: upserted %d awards", count)
        return count


class CeddAwardsScraper:
    """CEDD contracts awarded (rolling 6 months; detail page has contractor+sum)."""

    def __init__(self, conn: sqlite3.Connection, session: requests.Session):
        self.conn = conn
        self.session = session

    def scrape(self) -> int:
        log.info("CEDD: scraping awards from %s", CEDD_AWARDS_URL)
        resp = _retry_with_backoff(
            self.session.get, CEDD_AWARDS_URL, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        count = 0
        table = soup.find("table")
        if table is None:
            log.warning("CEDD: no award table found")
            return 0
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            ref_cell = cells[0]
            if "tender reference" in ref_cell.lower() and "subject" in cells[1].lower():
                continue
            subject = cells[1]
            award_date = _parse_date_iso(cells[2])
            # Tender ref is embedded like "Contract No.: NL/2026/08"
            m = re.search(r"contract no\.?:?\s*([A-Za-z0-9/]+)", ref_cell, re.I)
            tender_ref = m.group(1) if m else ref_cell
            anchor = tr.find("a", href=True)
            contractor = ""
            value = None
            if anchor:
                detail_url = urljoin(CEDD_DETAIL_BASE, anchor["href"])
                contractor, value = self._fetch_detail(detail_url)
            count += _award_insert(
                self.conn, tender_ref, award_date, contractor, value,
                "cedd", CEDD_AWARDS_URL,
            )
        self.conn.commit()
        log.info("CEDD: upserted %d awards", count)
        return count

    def _fetch_detail(self, detail_url: str) -> tuple[str, Optional[float]]:
        try:
            resp = _retry_with_backoff(
                self.session.get, detail_url, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()
        except Exception as exc:
            log.warning("CEDD: detail fetch failed %s: %s", detail_url, exc)
            return "", None
        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text("\n", strip=True)
        contractor = ""
        m = re.search(r"Contractor\s*:\s*\n?(.+?)\n", text)
        if m:
            contractor = m.group(1).strip()
        value = None
        m = re.search(r"Awarded Sum\s*\(million\)\s*:\s*\n?HK\$\s*([\d,]+(?:\.\d+)?)", text, re.I)
        if m:
            value = float(m.group(1).replace(",", "")) * 1_000_000
        return contractor, value


# --------------------------------------------------------------------------- #
# Cross-referencing
# --------------------------------------------------------------------------- #

def cross_reference_companies(
    conn: sqlite3.Connection, api: CrApiClient, delay: float = 0.5,
) -> int:
    unlinked = _get_unlinked_awards(conn)
    if not unlinked:
        log.info("Cross-ref: no unlinked awards")
        return 0

    log.info("Cross-ref: %d award(s) need company lookup", len(unlinked))
    linked = 0
    seen_brns: set[str] = set()

    for award_id, contractor_name in unlinked:
        brn = api.lookup_contractor(contractor_name)
        if brn:
            # Ensure the company row exists first so the FK on awards holds.
            _upsert_company(conn, brn=brn)
            _link_award_brn(conn, award_id, brn)
            linked += 1
            if brn not in seen_brns:
                seen_brns.add(brn)
            log.debug("Cross-ref: linked award %d → BRN %s (%s)",
                      award_id, brn, contractor_name[:60])
        else:
            log.debug("Cross-ref: no CR match for '%s'", contractor_name[:60])
        time.sleep(delay)

    # Also fetch full profiles for newly discovered companies
    for brn in seen_brns:
        try:
            results = api.search_by_name(brn)
            for rec in results:
                r_brn = rec.get("Brn") or rec.get("brn", "")
                if r_brn == brn:
                    _upsert_company(
                        conn,
                        brn=r_brn,
                        english_name=rec.get("English_Company_Name", rec.get("english_company_name", "")),
                        chinese_name=rec.get("Chinese_Company_Name", rec.get("chinese_company_name", "")),
                        registered_address=rec.get("Address_of_Registered_Office", rec.get("address_of_registered_office", "")),
                        company_type=rec.get("Company_Type", rec.get("company_type", "")),
                        date_of_incorporation=rec.get("Date_of_Incorporation", rec.get("date_of_incorporation", "")),
                    )
        except Exception as exc:
            log.debug("Cross-ref: profile fetch failed for BRN %s: %s", brn, exc)

    conn.commit()
    log.info("Cross-ref: linked %d/%d awards", linked, len(unlinked))
    return linked


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def run_cycle(
    conn: sqlite3.Connection,
    session: requests.Session,
    sources: Optional[set[str]] = None,
) -> dict:
    all_sources = {"devb", "ha", "gld", "cr_api", "works"}
    sources = sources or all_sources

    stats: dict = {}

    if "devb" in sources:
        try:
            scraper = DevbScraper(conn, session)
            stats["devb_contractors"] = scraper.scrape()
        except Exception as exc:
            log.exception("DEVB cycle failed: %s", exc)
            stats["devb_contractors"] = 0

    if "ha" in sources:
        try:
            scraper = HaScraper(conn)
            t, a = scraper.scrape_all()
            stats["ha_tenders"] = t
            stats["ha_awards"] = a
        except Exception as exc:
            log.exception("HA cycle failed: %s", exc)
            stats["ha_tenders"] = 0
            stats["ha_awards"] = 0

    if "gld" in sources:
        try:
            scraper = GldGazetteScraper(conn)
            stats["gld_tenders"] = scraper.scrape()
        except Exception as exc:
            log.exception("GLD cycle failed: %s", exc)
            stats["gld_tenders"] = 0

    if "works" in sources:
        for cls, key in [
            (ArchsdAwardsScraper, "archsd_awards"),
            (GldCsvAwardsScraper, "gld_csv_awards"),
            (HydAwardsScraper, "hyd_awards"),
            (CeddAwardsScraper, "cedd_awards"),
        ]:
            try:
                scraper = cls(conn, session)
                stats[key] = scraper.scrape()
            except Exception as exc:
                log.exception("%s cycle failed: %s", key, exc)
                stats[key] = 0

    # Cross-reference whenever awards may have been added
    if "cr_api" in sources or "ha" in sources or "works" in sources:
        try:
            api = CrApiClient(session)
            stats["cross_ref_linked"] = cross_reference_companies(conn, api)
        except Exception as exc:
            log.exception("Cross-ref cycle failed: %s", exc)
            stats["cross_ref_linked"] = 0

    conn.commit()
    return stats


# --------------------------------------------------------------------------- #
# Watch loop
# --------------------------------------------------------------------------- #

def _watch_loop(args: argparse.Namespace) -> None:
    interval = args.interval_hours * 3600
    conn = _init_db(DB_PATH)
    session = _http_session()
    sources = _parse_sources(args.source)

    log.info("Continuous mode: checking every %.1f hour(s). Ctrl-C to stop.",
             args.interval_hours)
    try:
        while True:
            try:
                stats = run_cycle(conn, session, sources)
                log.info("Cycle complete: %s", stats)
                if args.export_csv:
                    _export_csvs(conn, PROCESSED_DIR)
            except KeyboardInterrupt:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("Watch cycle failed: %s", exc)
            log.info("Next check in %.1f hour(s)", args.interval_hours)
            time.sleep(interval)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _parse_sources(source_arg: Optional[str]) -> Optional[set[str]]:
    if not source_arg:
        return None
    return {s.strip().lower() for s in source_arg.split(",")}


def _discover_mode(args: argparse.Namespace) -> int:
    session = _http_session()
    sources = _parse_sources(args.source) or {"devb", "ha", "gld", "cr_api", "works"}

    if "devb" in sources:
        print("--- DEVB Approved Contractors ---")
        try:
            scraper = DevbScraper(None, session)  # type: ignore[arg-type]
            urls = scraper.discover_js_files()
            print(f"  Discovered {len(urls)} JS data file(s)")
            for u in urls:
                print(f"    {u}")
            if urls:
                records = scraper.parse_js_file(urls[0])
                print(f"  Sample ({len(records)} records from first file):")
                for r in records[:3]:
                    print(f"    {r}")
        except Exception as exc:
            print(f"  FAILED: {exc}")

    if "cr_api" in sources:
        print("--- Companies Registry API ---")
        try:
            api = CrApiClient(session)
            results = api.search_by_name("Gammon")
            print(f"  Test query 'Gammon': {len(results)} result(s)")
            for r in results[:3]:
                print(f"    {r.get('English_Company_Name', r.get('english_company_name', '?'))} "
                      f"Brn={r.get('Brn', r.get('brn', '?'))}")
        except Exception as exc:
            print(f"  FAILED: {exc}")

    if "ha" in sources:
        print("--- Housing Authority ---")
        try:
            html = _render_page_with_playwright(HA_TENDER_URL)
            soup = BeautifulSoup(html, "lxml")
            tables = soup.find_all("table")
            print(f"  Tenders page: {len(tables)} table(s) found")
            if tables:
                rows = tables[0].find_all("tr")
                print(f"    First table: {len(rows)} row(s)")
                for row in rows[:3]:
                    cells = row.find_all(["td", "th"])
                    print(f"    {[c.get_text(strip=True)[:50] for c in cells]}")
        except Exception as exc:
            print(f"  FAILED: {exc}")

    if "gld" in sources:
        print("--- GLD eGazette ---")
        try:
            html = _render_page_with_playwright(GLD_EGAZETTE_URL)
            soup = BeautifulSoup(html, "lxml")
            tables = soup.find_all("table")
            print(f"  eGazette page: {len(tables)} table(s) found")
            if tables:
                rows = tables[0].find_all("tr")
                print(f"    First table: {len(rows)} row(s)")
                for row in rows[:3]:
                    cells = row.find_all(["td", "th"])
                    print(f"    {[c.get_text(strip=True)[:50] for c in cells]}")
        except Exception as exc:
            print(f"  FAILED: {exc}")

    if "works" in sources:
        print("--- Contract-award archives (works depts + GLD) ---")
        for cls, label, url in [
            (ArchsdAwardsScraper, "ArchSD", ARCHSD_AWARDS_URL),
            (GldCsvAwardsScraper, "GLD CSV", GLD_AWARDS_CSV_URL),
            (HydAwardsScraper, "HyD", HYD_AWARDS_URL),
            (CeddAwardsScraper, "CEDD", CEDD_AWARDS_URL),
        ]:
            try:
                resp = session.get(url, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                n = len(resp.text)
                print(f"  {label}: HTTP {resp.status_code}, {n} bytes")
            except Exception as exc:
                print(f"  {label}: FAILED: {exc}")

    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Scrape HK procurement data: DEVB contractors, HA tenders/awards, "
                    "GLD eGazette notices, Companies Registry profiles."
    )
    p.add_argument("--watch", action="store_true",
                   help="run continuously, re-checking for new data")
    p.add_argument("--interval-hours", type=float, default=24.0,
                   help="poll cadence for --watch (default 24 = daily)")
    p.add_argument("--discover", action="store_true",
                   help="test source connectivity and exit (no writes)")
    p.add_argument("--source", type=str, default=None,
                   help="comma-separated sources: devb,ha,gld,cr_api,works (default: all)")
    p.add_argument("--export-csv", action="store_true",
                   help="export SQLite tables to CSV after scraping")
    p.add_argument("--compile", action="store_true",
                   help="compile tenders + awards into one date-sorted CSV")
    p.add_argument("--skip-download", action="store_true",
                   help="re-process from cached rendered HTML (no network)")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    if args.discover:
        return _discover_mode(args)

    conn = _init_db(DB_PATH)
    session = _http_session()
    sources = _parse_sources(args.source)

    try:
        if args.watch:
            _watch_loop(args)
            return 0

        if args.skip_download:
            log.info("Skipping download/scrape; using existing database")
        else:
            stats = run_cycle(conn, session, sources)
            log.info("Scrape complete: %s", stats)

        for table in ["devb_contractors", "tenders", "awards", "companies"]:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            log.info("Table %s: %d rows", table, count)

        if args.export_csv:
            _export_csvs(conn, PROCESSED_DIR)

        if args.compile:
            compile_unified_awards(conn, PROCESSED_DIR)

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())