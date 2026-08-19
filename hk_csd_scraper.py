#!/usr/bin/env python3
"""Headless scraper + cleaner for HK C&SD construction statistics (product B1060005).

Downloads all historical construction resource tables (2003 -> latest) from the
Census and Statistics Department subject page, cleans them into a tidy long format,
and exports CSV + Parquet. Supports a foreground ``--watch`` loop for continuous,
incremental ingestion (default daily).

The C&SD site renders its download links with JavaScript, so the downloader uses
Playwright to render the page and then scrapes every ``.xlsx/.xls/.csv`` link at
runtime rather than hard-coding URLs. Run ``--discover`` first on a live network to
confirm the discovered links and, if needed, extend ``PRODUCT_CODES`` below.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional
from urllib.parse import urljoin, urlparse, unquote
from xml.etree import ElementTree as ET

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SOURCE_URL = (
    "https://www.censtatd.gov.hk/en/EIndexbySubject.html?scode=330&pcode=B1060005"
)
SUBJECT_CODE = "330"
# Primary product code for "Report on Annual Survey of Construction Activities".
# Add other construction-series codes here as discovered (e.g. construction
# materials/labour cost indices, gross value of construction work performed).
PRODUCT_CODES = ["B1060005"]

RAW_DIR = Path("./data/raw")
PROCESSED_DIR = Path("./data/processed")
MANIFEST_PATH = Path("./data/manifest.json")

START_YEAR = 2003
END_YEAR = 2026
REQUEST_TIMEOUT = 60
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Final tidy schema.
OUTPUT_COLUMNS = [
    "period",
    "period_type",
    "category",
    "indicator",
    "value",
    "unit",
    "source_file",
]

# HK C&SD missing/suppression tokens.
MISSING_TOKENS = {
    "N.A.", "N/A", "NA", "n.a.", "n/a", "NIL", "Nil", "nil",
    "-", "—", "–", "*", "**", "#", "..", "...", "..*",
}

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

log = logging.getLogger("hk_csd_scraper")


# --------------------------------------------------------------------------- #
# Networking helpers
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
    """Call ``fn`` with exponential backoff + jitter on transient failures."""
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except exceptions as exc:  # noqa: BLE001 - retry broadly, re-raise finally
            last_exc = exc
            if attempt == retries - 1:
                break
            delay = min(max_delay, base_delay * (2 ** attempt)) + random.uniform(0, 0.5)
            log.warning("Attempt %d/%d failed (%s); retrying in %.1fs",
                        attempt + 1, retries, exc, delay)
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


def _http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _render_page_html(url: str, dump_path: Optional[Path] = None) -> str:
    """Return rendered HTML using Playwright; fall back to a plain GET.

    If ``dump_path`` is set, the HTML is written there for manual inspection.
    """
    backend = "requests"
    try:
        from playwright.sync_api import sync_playwright  # imported lazily

        def _render():
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                try:
                    page = browser.new_page(user_agent=USER_AGENT)
                    try:
                        page.goto(url, wait_until="networkidle", timeout=45000)
                    except Exception:  # noqa: BLE001 - networkidle can be flaky
                        page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    page.wait_for_timeout(4000)  # allow JS-rendered links to appear
                    return page.content()
                finally:
                    browser.close()

        html = _retry_with_backoff(_render, retries=3)
        backend = "playwright"
    except Exception as exc:  # noqa: BLE001 - Playwright may be missing/blocked
        log.warning(
            "Playwright render failed (%s); falling back to a plain GET. "
            "If the page is JS-rendered, run: "
            "`pip install playwright && playwright install chromium`", exc,
        )
        resp = _retry_with_backoff(
            _http_session().get, url, timeout=REQUEST_TIMEOUT,
            exceptions=(requests.RequestException,),
        )
        resp.raise_for_status()
        html = resp.text

    if dump_path is not None:
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        dump_path.write_text(html, encoding="utf-8")
        log.info("Saved rendered HTML (%d chars) to %s", len(html), dump_path)
    log.info("Rendered page via %s backend (%d chars)", backend, len(html))
    return html


def _is_download_url(url: str) -> bool:
    low = url.lower()
    if re.search(r"\.pdf(\?|#|$)", low):
        return False
    if re.search(r"\.(xlsx|xls|csv|zip|tsv)(\?|#|$)", low):
        return True
    # C&SD serves attachments under an /att/ (attachment) path segment.
    path = urlparse(url).path.lower()
    if "/att/" in path or "download" in path or "attach" in path:
        return True
    return False


def _discover_links(html: str, base_url: str) -> list[str]:
    """Extract candidate download URLs from anchors, JS handlers and script text."""
    soup = BeautifulSoup(html, "lxml")
    found: set[str] = set()
    seen: list[str] = []

    def _consider(href: str) -> None:
        href = (href or "").strip()
        if not href or href.lower().startswith(("javascript:", "mailto:", "#")):
            return
        seen.append(href)
        full = urljoin(base_url, href)
        if _is_download_url(full):
            found.add(full)

    for a in soup.find_all("a", href=True):
        _consider(a.get("href"))

    for tag in soup.find_all(attrs={"onclick": True}):
        for m in re.finditer(r"['\"]([^'\"]*\.(?:xlsx|xls|csv|zip)[^'\"]*)['\"]",
                             tag["onclick"], re.IGNORECASE):
            _consider(m.group(1))

    for tag in soup.find_all():
        for attr in ("data-href", "data-url", "data-download", "data-file"):
            if tag.has_attr(attr):
                _consider(tag[attr])

    for s in soup.find_all("script"):
        text = s.string or s.get_text() or ""
        for m in re.finditer(r"(?:https?:)?//[^\"'\s)]+\.(?:xlsx|xls|csv|zip)[^\"'\s)]*",
                             text, re.IGNORECASE):
            _consider(m.group(0))

    if not found:
        log.warning("No download URLs matched. Inspected %d href(s); a sample:", len(seen))
        for href in seen[:20]:
            log.warning("  href: %s", href)
    return sorted(found)


def _product_code_from_url(url: str) -> str:
    m = re.search(r"([A-Z]\d{7})", url, re.IGNORECASE)
    return m.group(1).upper() if m else "unknown"


def _year_from_url(url: str) -> Optional[int]:
    m = re.search(r"(20\d{2})", unquote(url))
    return int(m.group(1)) if m else None


# --------------------------------------------------------------------------- #
# Manifest (idempotency + incremental state)
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


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- #
# Downloader
# --------------------------------------------------------------------------- #

def download_raw_data(
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    discover_only: bool = False,
    dump_html: Optional[Path] = None,
) -> list[Path]:
    """Discover and download all raw files; returns local paths (or [] if dry-run)."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest()
    session = _http_session()

    log.info("Rendering subject page: %s", SOURCE_URL)
    html = _render_page_html(SOURCE_URL, dump_html)
    links = _discover_links(html, SOURCE_URL)
    log.info("Discovered %d downloadable link(s)", len(links))

    if not links:
        log.warning(
            "No download links discovered. If you can see them in a browser, the page "
            "is likely JS-rendered and Playwright/Chromium is not set up — run "
            "`pip install playwright && playwright install chromium`, or use --dump-html "
            "to inspect the rendered page and adjust _is_download_url()."
        )

    if discover_only:
        for url in links:
            print(f"{_product_code_from_url(url):10s}  {url}")
        return []

    # Per-year progress + missing-period reporting over the requested range.
    years_seen = {_year_from_url(u) for u in links if _year_from_url(u)}
    for year in range(start_year, end_year + 1):
        if year in years_seen:
            log.info("Year %d: %d file(s) found", year,
                     sum(1 for u in links if _year_from_url(u) == year))
        else:
            log.warning("Year %d: no file discovered (missing period)", year)

    downloaded: list[Path] = []
    for url in links:
        pcode = _product_code_from_url(url)
        fname = unquote(Path(urlparse(url).path).name) or f"{pcode}_download"
        if not re.search(r"\.(xlsx|xls|csv|zip)$", fname, re.IGNORECASE):
            fname += ".xlsx"
        dest = RAW_DIR / pcode / fname
        entry = manifest["files"].get(url)

        if dest.exists():
            if entry and entry.get("sha256") == _sha256_of_file(dest):
                log.debug("Skipping (already downloaded): %s", dest.name)
            else:
                manifest["files"][url] = _record_entry(dest, url)
                log.info("Recorded existing file: %s", dest.name)
            continue

        log.info("Downloading [%s] %s", pcode, fname)
        try:
            _download_once(session, url, dest)
            manifest["files"][url] = _record_entry(dest, url)
            downloaded.append(dest)
        except Exception as exc:  # noqa: BLE001 - keep going on individual failures
            log.error("Failed to download %s: %s", url, exc)

    # Extract any ZIP archives (the 2021/2022 CLBM data ships as XML inside zips).
    for url in links:
        if url.lower().endswith(".zip"):
            pcode = _product_code_from_url(url)
            fname = unquote(Path(urlparse(url).path).name)
            zip_path = RAW_DIR / pcode / fname
            if zip_path.exists():
                _extract_zip(zip_path)

    _save_manifest(manifest)
    log.info("Downloaded %d new file(s); %d total tracked",
             len(downloaded), len(manifest["files"]))
    return downloaded


def _extract_zip(zip_path: Path) -> list[Path]:
    out_dir = zip_path.with_suffix("")
    if out_dir.exists():
        return sorted(out_dir.rglob("*"))
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    extracted = sorted(out_dir.rglob("*"))
    log.info("Extracted %d file(s) from %s", len(extracted), zip_path.name)
    return extracted


def _record_entry(dest: Path, url: str) -> dict:
    return {
        "path": str(dest),
        "sha256": _sha256_of_file(dest),
        "size": dest.stat().st_size,
        "first_seen": datetime.now().isoformat(timespec="seconds"),
        "url": url,
    }


def _download_once(session: requests.Session, url: str, dest: Path) -> None:
    def _get():
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 16):
                    fh.write(chunk)
        return dest

    _retry_with_backoff(
        _get, retries=5, exceptions=(requests.RequestException, OSError)
    )


# --------------------------------------------------------------------------- #
# Cleaner
# --------------------------------------------------------------------------- #

class DataCleaner:
    """Turn raw C&SD tables into one tidy, SQL/pgvector-ready long table."""

    def __init__(self, raw_dir: Path = RAW_DIR, processed_dir: Path = PROCESSED_DIR):
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)

    # -- reading ---------------------------------------------------------- #

    def _read_raw(self, path: Path) -> pd.DataFrame:
        if path.suffix.lower() in (".xlsx", ".xls"):
            return pd.read_excel(path, sheet_name=0, header=None, dtype=object)
        return self._read_csv_fallback(path)

    def _read_csv_fallback(self, path: Path) -> pd.DataFrame:
        raw = path.read_bytes()
        for enc in ("utf-8-sig", "utf-8", "big5", "latin-1"):
            try:
                import io
                return pd.read_csv(io.BytesIO(raw), header=None, dtype=object,
                                   encoding=enc)
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        return pd.read_csv(path, header=None, dtype=object, encoding="latin-1")

    # -- helpers ---------------------------------------------------------- #

    @staticmethod
    def _not_empty(v) -> bool:
        if v is None or pd.isna(v):
            return False
        s = str(v).strip()
        return s != "" and s not in MISSING_TOKENS

    @staticmethod
    def _looks_numeric(v) -> bool:
        if v is None or pd.isna(v):
            return False
        s = str(v).strip()
        if not s or s in MISSING_TOKENS:
            return False
        try:
            float(s.replace(",", "").replace(" ", ""))
            return True
        except ValueError:
            return False

    @staticmethod
    def _snake_case(name: str) -> str:
        name = re.sub(r"[()（）\[\]【】]", " ", str(name))
        name = name.replace("&", " and ").replace("%", " pct ")
        name = re.sub(r"[^0-9A-Za-z]+", "_", name).strip("_").lower()
        return re.sub(r"_+", "_", name)

    @staticmethod
    def _is_title_row(row: list, ncols: int) -> bool:
        non_empty = [str(v).strip() for v in row if DataCleaner._not_empty(v)]
        return len(non_empty) <= 1 or len(set(non_empty)) == 1

    def _find_header(self, rows: list[list], ncols: int) -> tuple[list[int], int]:
        """Return header row indices and the index of the first data row."""
        data_start = None
        for i, row in enumerate(rows):
            if sum(1 for v in row if self._looks_numeric(v)) >= 2:
                data_start = i
                break
        if data_start is None:
            return [], 0
        header_rows: list[int] = []
        j = data_start - 1
        while j >= 0 and len(header_rows) < 2:
            if not self._is_title_row(rows[j], ncols) and any(
                self._not_empty(v) for v in rows[j]
            ):
                header_rows.insert(0, j)
            j -= 1
        return header_rows, data_start

    def _build_column_names(self, rows: list[list], header_rows: list[int], ncols: int) -> list[str]:
        # Forward-fill empty cells within each header row to reconstruct merged
        # Excel group labels (e.g. "Construction Materials Cost Index" spanning
        # "All items / Sand / Cement").
        effective: list[list[str]] = []
        for r in header_rows:
            labels: list[str] = []
            carry = ""
            for c in range(ncols):
                v = rows[r][c]
                if self._not_empty(v):
                    carry = str(v).strip()
                labels.append(carry)
            effective.append(labels)

        col_names: list[str] = []
        for c in range(ncols):
            parts: list[str] = []
            for r in range(len(header_rows)):
                lab = effective[r][c]
                if lab and lab not in parts:
                    parts.append(lab)
            name = "_".join(parts) if parts else f"col_{c}"
            col_names.append(self._snake_case(name))
        return col_names

    @staticmethod
    def _classify_indicator(name: str) -> str:
        n = name.lower()
        if "material" in n and ("index" in n or "cost" in n):
            return "material_cost_index"
        if any(k in n for k in ("labour", "labor", "wage")) and ("index" in n or "cost" in n):
            return "labor_cost_index"
        if "gross value" in n or "gross_value" in n:
            return "gross_value_of_construction_work"
        if "output" in n or "value of construction" in n:
            return "output"
        if "employment" in n or "persons engaged" in n:
            return "employment"
        return "other"

    @staticmethod
    def _extract_unit(name: str) -> str:
        m = re.search(r"\(([^()]*)\)\s*$", str(name))
        if m:
            return m.group(1).strip()
        low = str(name).lower()
        if "index" in low:
            return "index"
        return ""

    def _parse_period(self, value) -> Optional[tuple[str, str]]:
        s = str(value).strip()
        if not s or s in MISSING_TOKENS:
            return None
        s = (s.replace("年", "-").replace("月", "").replace("季", "Q")
             .replace("第", "").replace("／", "/").replace(" ", ""))

        # Quarterly: 2024-Q1 | 2024Q1 | Q1-2024 | Q1/2024
        m = re.match(r"^(\d{4})[-/]?Q([1-4])$", s) or re.match(r"^Q([1-4])[-/]?(\d{4})$", s)
        if m:
            if m.group(1).isdigit() and len(m.group(1)) == 4:
                year, q = m.group(1), m.group(2)
            else:
                q, year = m.group(1), m.group(2)
            return f"{year}-Q{q}", "quarterly"

        # Monthly: 2024-12 | 2024/12 | Dec-2024 | 12/2024
        m = re.match(r"^(\d{4})[-/](\d{1,2})$", s)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}", "monthly"
        m = re.match(r"^(\d{1,2})[-/](\d{4})$", s)
        if m and 1 <= int(m.group(1)) <= 12:
            return f"{int(m.group(2)):04d}-{int(m.group(1)):02d}", "monthly"
        m = re.match(r"^([A-Za-z]{3})[-/]?(\d{4})$", s)
        if m and m.group(1).lower() in _MONTH_MAP:
            return f"{int(m.group(2)):04d}-{_MONTH_MAP[m.group(1).lower()]:02d}", "monthly"

        # Annual: 2024
        if re.match(r"^\d{4}$", s):
            return s, "annual"

        return None

    # -- pipeline --------------------------------------------------------- #

    def clean_file(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            raw = self._read_raw(path)
        except Exception as exc:  # noqa: BLE001
            log.error("Could not read %s: %s", path.name, exc)
            return None

        raw = raw.dropna(how="all").dropna(axis=1, how="all")
        if raw.empty:
            log.warning("Empty table: %s", path.name)
            return None

        rows = raw.values.tolist()
        ncols = raw.shape[1]
        header_rows, data_start = self._find_header(rows, ncols)
        if data_start == 0:
            log.warning("No numeric data block detected in %s; skipping", path.name)
            return None

        col_names = self._build_column_names(rows, header_rows, ncols)

        data = raw.iloc[data_start:].copy()
        data.columns = col_names

        period_col = next(
            (c for c, n in enumerate(col_names)
             if re.search(r"period|year|month|quarter", n, re.IGNORECASE)),
            0,
        )
        value_cols = [
            c for c in range(ncols)
            if c != period_col and not re.search(r"note|remark|footnote", col_names[c], re.IGNORECASE)
        ]
        if not value_cols:
            log.warning("No value columns in %s; skipping", path.name)
            return None

        periods = data.iloc[:, period_col].map(self._parse_period)
        data["period"] = periods.map(lambda x: x[0] if x else None)
        data["period_type"] = periods.map(lambda x: x[1] if x else None)
        data = data.dropna(subset=["period"])

        id_vars = ["period", "period_type"]
        keep = id_vars + [col_names[c] for c in value_cols]
        sub = data[keep].copy()

        for c in [col_names[c] for c in value_cols]:
            sub[c] = sub[c].astype(str).str.strip().replace(list(MISSING_TOKENS), np.nan)

        tidy = sub.melt(id_vars=id_vars, var_name="indicator", value_name="value")
        tidy["value"] = pd.to_numeric(
            tidy["value"].astype(str).str.replace(",", "", regex=False), errors="coerce"
        )
        tidy = tidy.dropna(subset=["value"])

        tidy["category"] = tidy["indicator"].map(self._classify_indicator)
        tidy["unit"] = tidy["indicator"].map(self._extract_unit)
        tidy["source_file"] = path.name
        tidy = tidy[OUTPUT_COLUMNS]
        return tidy

    def _parse_title_period(self, values) -> Optional[tuple[str, str]]:
        for v in values:
            if v is None or pd.isna(v):
                continue
            s = str(v).strip()
            if not s:
                continue
            m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", s)
            if m:
                return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}", "monthly"
            m = re.match(r"^([A-Za-z]+)\s*(\d{4})$", s)
            if m and m.group(1).lower() in _MONTH_FULL:
                return f"{int(m.group(2)):04d}-{_MONTH_FULL[m.group(1).lower()]:02d}", "monthly"
            m = re.match(r"^([A-Za-z]{3})[-/\s](\d{2})$", s)
            if m and m.group(1).lower() in _MONTH_MAP:
                return f"{2000 + int(m.group(2)):04d}-{_MONTH_MAP[m.group(1).lower()]:02d}", "monthly"
        return None

    def _clean_material_price(self, path: Path) -> Optional[pd.DataFrame]:
        """Parse the bilingual 'Average Wholesale Prices of Selected Building Materials' tables."""
        raw = self._read_raw(path)
        raw = raw.dropna(how="all").dropna(axis=1, how="all")
        if raw.empty:
            return None

        nrows, ncols = raw.shape
        header_row = None
        for i in range(nrows):
            cells = [str(c).strip() for c in raw.iloc[i].tolist() if pd.notna(c)]
            joined = " | ".join(cells)
            if "Materials" in joined and ("HK$" in joined or "Unit" in joined):
                header_row = i
                break
        if header_row is None:
            for i in range(nrows):
                if any("HK$" in str(c) for c in raw.iloc[i].tolist() if pd.notna(c)):
                    header_row = i
                    break
        if header_row is None:
            return None

        header = [str(c).strip() if pd.notna(c) else "" for c in raw.iloc[header_row].tolist()]
        name_col = next((i for i, h in enumerate(header) if h.lower() == "materials"), None)
        unit_col = next((i for i, h in enumerate(header) if h.lower() == "unit"), None)
        price_col = next((i for i, h in enumerate(header) if "hk$" in h.lower()), None)
        if name_col is None or price_col is None:
            return None

        title_values = [raw.iat[r, c] for r in range(header_row) for c in range(ncols)]
        pp = self._parse_title_period(title_values)
        if pp is None:
            return None
        period, ptype = pp

        rows: list[dict] = []
        for r in range(header_row + 1, nrows):
            price = raw.iat[r, price_col]
            if not self._looks_numeric(price):
                continue
            name = str(raw.iat[r, name_col]).strip() if pd.notna(raw.iat[r, name_col]) else ""
            unit = str(raw.iat[r, unit_col]).strip() if (unit_col is not None and pd.notna(raw.iat[r, unit_col])) else ""
            name = re.sub(r"[\*\^@#]+", "", name)
            name = re.sub(r"\s+", " ", name).strip()
            rows.append({
                "period": period,
                "period_type": ptype,
                "category": "material_wholesale_price",
                "indicator": name,
                "value": float(str(price).replace(",", "").strip()),
                "unit": unit,
                "source_file": path.name,
            })
        if not rows:
            return None
        return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    @staticmethod
    def _load_item_map(metadata_path: Path) -> dict[str, tuple[str, str]]:
        """Build ITEM_P code -> (English name, unit) from the CLBM metadata XML."""
        mapping: dict[str, tuple[str, str]] = {}
        try:
            root = ET.parse(metadata_path).getroot()
        except (ET.ParseError, OSError):
            return mapping
        for it in root.iter("item"):
            d = {c.tag: (c.text or "").strip() for c in it}
            if d.get("CLASS_VAR") != "item_p":
                continue
            code = d.get("CLASS_CODE")
            desc = d.get("CLASS_CODE_DESC_ENG", "")
            if not code or not desc:
                continue
            unit = ""
            m = re.search(r"\[([^\]]+)\]\s*$", desc)
            if m:
                unit = m.group(1).strip()
                desc = desc[:m.start()].rstrip(", ").strip()
            mapping[code] = (desc, unit)
        return mapping

    def _clean_clbm_xml(self, path: Path) -> Optional[pd.DataFrame]:
        """Parse the SDMX-style CLBM masterdata XML shipped in the 2021/2022 zips."""
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as exc:
            log.error("XML parse failed %s: %s", path.name, exc)
            return None

        item_map = self._load_item_map(path.parent / "CLBM_Metadata.xml")

        rows: list[dict] = []
        for it in root.iter("item"):
            fields = {}
            for c in it:
                if c.tag in ("CCYY", "MM", "ITEM_P", "STAT_VAR", "STAT_PRES", "STAT_VALUE"):
                    fields[c.tag] = (c.text or "").strip()
            y, mm, val = fields.get("CCYY"), fields.get("MM"), fields.get("STAT_VALUE")
            if not y or not self._looks_numeric(val):
                continue
            code = fields.get("ITEM_P", "")
            statvar = fields.get("STAT_VAR", "")
            name, unit = item_map.get(code, (code, ""))
            indicator = name if statvar in ("", "PRICE") else f"{name} ({statvar})"
            rows.append({
                "period": f"{int(y):04d}-{int(mm or '1'):02d}",
                "period_type": "monthly",
                "category": "material_wholesale_price",
                "indicator": indicator,
                "value": float(str(val).replace(",", "").strip()),
                "unit": unit,
                "source_file": path.name,
            })
        if not rows:
            return None
        return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    def run(self) -> pd.DataFrame:
        files = sorted(
            p for p in self.raw_dir.rglob("*")
            if p.suffix.lower() in (".xlsx", ".xls", ".csv", ".xml")
        )
        if not files:
            log.warning("No raw files under %s; nothing to clean", self.raw_dir)
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        frames = []
        for i, p in enumerate(files, 1):
            log.info("[%d/%d] cleaning %s", i, len(files), p)
            frame = None
            if p.suffix.lower() == ".xml":
                if "masterdata" in p.stem.lower():
                    frame = self._clean_clbm_xml(p)
            elif p.suffix.lower() in (".xlsx", ".xls", ".csv"):
                frame = self._clean_material_price(p)
                if frame is None:
                    frame = self.clean_file(p)  # generic fallback
            if frame is not None and not frame.empty:
                frames.append(frame)

        if not frames:
            log.warning("No rows produced; returning empty schema")
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        out = pd.concat(frames, ignore_index=True)
        out = out.drop_duplicates()
        out = out.sort_values(["period", "category", "indicator"]).reset_index(drop=True)
        return out


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #

def clean_data(
    raw_dir: Path = RAW_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Clean all raw files and write CSV + Parquet to ``processed_dir``."""
    cleaner = DataCleaner(raw_dir, processed_dir)
    result = cleaner.run()

    processed_dir.mkdir(parents=True, exist_ok=True)
    csv_path = processed_dir / "hk_construction_resources_2003_2026_cleaned.csv"
    parquet_path = processed_dir / "hk_construction_resources_2003_2026_cleaned.parquet"

    result.to_csv(csv_path, index=False)
    result.to_parquet(parquet_path, index=False)

    log.info("Wrote %d rows to:\n  %s\n  %s", len(result), csv_path, parquet_path)
    return result


# --------------------------------------------------------------------------- #
# Watch loop
# --------------------------------------------------------------------------- #

def _watch_loop(args: argparse.Namespace) -> None:
    interval = args.interval_hours * 3600
    log.info("Continuous mode: checking every %.1f hour(s). Ctrl-C to stop.", args.interval_hours)
    while True:
        try:
            download_raw_data(args.start_year, args.end_year)
            clean_data()
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001 - a bad cycle must not kill the loop
            log.exception("Watch cycle failed: %s", exc)
        log.info("Next check in %.1f hour(s)", args.interval_hours)
        time.sleep(interval)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Scrape + clean HK C&SD construction statistics (2003 -> latest)."
    )
    p.add_argument("--start-year", type=int, default=START_YEAR)
    p.add_argument("--end-year", type=int, default=END_YEAR)
    p.add_argument("--watch", action="store_true",
                   help="run continuously, re-checking for new uploads")
    p.add_argument("--interval-hours", type=float, default=24.0,
                   help="poll cadence for --watch (default 24 = daily)")
    p.add_argument("--discover", action="store_true",
                   help="dry-run: list discovered download links and exit")
    p.add_argument("--dump-html", type=Path, default=None,
                   help="save the rendered page HTML to this path (for debugging discovery)")
    p.add_argument("--skip-download", action="store_true",
                   help="clean from existing ./data/raw without downloading")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    if args.watch:
        _watch_loop(args)
        return 0

    if args.discover:
        download_raw_data(args.start_year, args.end_year,
                          discover_only=True, dump_html=args.dump_html)
        return 0

    if not args.skip_download:
        download_raw_data(args.start_year, args.end_year, dump_html=args.dump_html)

    clean_data()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
