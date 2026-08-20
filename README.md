# QS_resource_material

Headless Python pipeline that scrapes, downloads, and cleans **Hong Kong Census & Statistics Department (C&SD)** construction statistics — specifically, the **Average Wholesale Prices of Selected Building Materials** (product code **B1060005**) published monthly by C&SD.

## Coverage

- **Machine-readable data**: 2013-01 → 2026-05 (161 contiguous months, zero gaps)
- **Earlier history** (2003–2012) is published as PDF only on the C&SD site; this pipeline focuses on the structured machine-readable formats (CSV, XLSX, XML) shipped as "accompanying files."

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Download the Playwright browser (required — the C&SD page is JS-rendered)
playwright install chromium

# 3. One-shot backfill (download all + clean + export)
python hk_csd_scraper.py

# 4. Continuous mode — check daily for new monthly releases
python hk_csd_scraper.py --watch
```

## CLI reference

```
python hk_csd_scraper.py [--start-year Y] [--end-year Y]
                          [--watch] [--interval-hours H]
                          [--discover] [--dump-html PATH]
                          [--skip-download] [--log-level LEVEL]
```

| Flag | Purpose |
|---|---|
| `--watch` | Foreground continuous loop; re-checks for new uploads |
| `--interval-hours 6` | Override poll cadence (default 24 = daily) |
| `--discover` | Dry-run: list discovered download links and exit |
| `--dump-html PATH` | Save the rendered page HTML for debugging discovery |
| `--skip-download` | Clean from existing `./data/raw` without network |
| `--start-year` / `--end-year` | Limit the reported year range (default 2003–2026) |

## How it works

### Downloader

1. Renders the C&SD subject page with Playwright (Chromium, headless).
2. Discovers all `/att/` (attachment) links matching `.csv`, `.xlsx`, `.xls`, or `.zip` — **PDFs are skipped**.
3. Downloads each file with exponential-backoff retries into `./data/raw/B1060005/`.
4. Extracts the two XML zips (2021/2022 CLBM masterdata — SDMX-style time series).
5. A SHA-256 manifest at `./data/manifest.json` tracks every URL, making re-runs idempotent (skips already-downloaded files) and the `--watch` loop incremental.

### Cleaner

Handles three real C&SD formats:

| Format | Period | Description |
|---|---|---|
| **CSV** (Big5) | 2023-01 → 2026-05 | Bilingual "Average Wholesale Prices" table — locates `Materials`/`Unit`/`HK$` columns, strips footnote markers, parses `Jan-23` / `JANUARY 2023` / `2023 年 1 月` periods |
| **XLSX** | 2023-01 → 2026-05 | Same data as CSV, bilingual layout (redundancy + corrupt file resilience) |
| **XML** (`CLBM_Masterdata.xml`) | 2013-01 → 2022 | SDMX time series parsed via `CLBM_Metadata.xml` code list to map `ITEM_P` codes → English material names + quantity units |

All formats are melted into a unified tidy (long) table and deduplicated.

### Output schema

```
period, period_type, category, indicator, value, unit, source_file
```

| Column | Example |
|---|---|
| `period` | `2024-01` |
| `period_type` | `monthly` |
| `category` | `material_wholesale_price` |
| `indicator` | `Portland cement (ordinary)` |
| `value` | `931.0` |
| `unit` | `tonne` |
| `source_file` | `B10600052024MM01B0100.csv` |

Exported to both **CSV** and **Parquet** under `./data/processed/`.

## Directory layout

```
QS_resource_material/
├── hk_csd_scraper.py              # C&SD construction material prices
├── hk_procurement_scraper.py      # HK procurement/tender/company data
├── requirements.txt
├── data/
│   ├── manifest.json              # C&SD download state
│   ├── procurement_manifest.json  # procurement download state
│   ├── raw/
│   │   ├── B1060005/              # 41 CSV + 41 XLSX + 2 extracted XML directories
│   │   ├── procurement_ha/        # cached rendered HTML
│   │   └── procurement_gld/       # cached rendered HTML
│   ├── processed/
│   │   ├── hk_construction_resources_2003_2026_cleaned.csv
│   │   └── hk_construction_resources_2003_2026_cleaned.parquet
│   └── procurement/
│       ├── hk_procurement.db
│       ├── devb_contractors.csv
│       ├── tenders.csv
│       └── awards.csv
└── README.md
```

## Data source

[HK Census and Statistics Department — Report on Annual Survey of Construction Activities](https://www.censtatd.gov.hk/en/EIndexbySubject.html?scode=330&pcode=B1060005)

The accompanying CSV/XLSX tables are "Average Wholesale Prices of Selected Building Materials." Unit abbreviations follow HK conventions (`no.` = per piece, `tonne` = metric ton).

## Dependencies

- Python 3.10+
- `requests`, `beautifulsoup4`, `lxml`
- `playwright` + Chromium browser
- `pandas`, `numpy`, `openpyxl`, `pyarrow`

---

## HK Procurement Scraper

A companion scraper (`hk_procurement_scraper.py`) that collects HK construction procurement data from four government sources into a shared SQLite database.

### Sources

| Source | Data | Method |
|---|---|---|
| **DEVB** | 545 approved contractors for public works (Buildings, Port Works, Roads & Drainage, Site Formation, Waterworks × Groups A/B/C with suspension flags) | HTTP GET JS data files |
| **Housing Authority** | 29 active construction tenders + 42 commercial property awards | Playwright render (SPA) |
| **GLD eGazette** | Government tender notices (Vue.js SPA — requires form-fill interaction; v1 logs a warning) | Playwright render |
| **Companies Registry Open API** | Company profiles (name, BRN, registered address, incorporation date) — free, no auth | REST API |

### Quick start

```bash
# Full backfill (all sources)
python hk_procurement_scraper.py

# Continuous mode — daily check for new tenders/awards
python hk_procurement_scraper.py --watch

# Test connectivity only (no writes)
python hk_procurement_scraper.py --discover

# Specific sources
python hk_procurement_scraper.py --source devb,ha

# Export SQLite → CSV
python hk_procurement_scraper.py --skip-download --export-csv
```

### CLI reference

```
python hk_procurement_scraper.py [--source devb,ha,gld,cr_api]
                                  [--watch] [--interval-hours H]
                                  [--discover] [--skip-download]
                                  [--export-csv] [--log-level LEVEL]
```

| Flag | Purpose |
|---|---|
| `--source S` | Comma-separated sources: `devb`, `ha`, `gld`, `cr_api` (default: all) |
| `--watch` | Foreground continuous loop; re-checks for new data |
| `--interval-hours 6` | Override poll cadence (default 24 = daily) |
| `--discover` | Test connectivity to each source and exit |
| `--skip-download` | Skip all network calls; just export CSVs from existing DB |
| `--export-csv` | Export all tables to CSV after scraping |
| `--log-level DEBUG` | Python logging level |

### Database schema

SQLite at `./data/procurement/hk_procurement.db` with four tables:

- **`devb_contractors`** — name_en, name_zh, category, group_code, status
- **`tenders`** — source, tender_ref, title_en, publication_date, closing_date, tender_url
- **`awards`** — tender_ref, award_date, contractor_name, contract_value, contract_value_currency
- **`companies`** — brn, english_name, chinese_name, registered_address, company_type, date_of_incorporation

### Output

```
data/procurement/
├── hk_procurement.db          # SQLite database (230 KB)
├── devb_contractors.csv       # 545 rows
├── tenders.csv                # 29 rows
├── awards.csv                 # 42 rows
└── companies.csv              # populated via cross-reference
```

### Known limitations

- **GLD eGazette** is a Vue.js SPA requiring form-fill interaction; v1 renders the government notices page but cannot extract results without search submission. Logs a warning and saves rendered HTML for inspection.
- **Companies Registry API** returns only basic profiles (name, address, BRN, type, incorporation date). Directors, shareholders, and filings require the paid e-Search Services portal.
- **HA commercial awards** use rowspan tables with continuation rows; column mapping is approximate. Raw HTML is preserved in the `raw_html` column.
- **Cross-referencing** (award contractor → CR API company) only works when `contractor_name` contains an actual company name. HA commercial awards are shop location/trade descriptions, not construction companies, so they correctly yield 0 matches.
- **BOQ line-item rates** were not found in any free public source; a placeholder schema table exists for future expansion.