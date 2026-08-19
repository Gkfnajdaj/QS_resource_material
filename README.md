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
├── hk_csd_scraper.py         # main script (download + clean + watch)
├── requirements.txt
├── data/
│   ├── manifest.json         # download state (idempotent + incremental)
│   ├── raw/
│   │   └── B1060005/         # 41 CSV + 41 XLSX + 2 extracted XML directories
│   └── processed/
│       ├── hk_construction_resources_2003_2026_cleaned.csv
│       └── hk_construction_resources_2003_2026_cleaned.parquet
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