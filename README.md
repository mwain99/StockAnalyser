# Stock Analyser

A PySpark pipeline that fetches daily stock price data for the S&P 500 top 100 companies from the Polygon.io REST API and performs four financial analyses.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Architecture Overview](#architecture-overview)
3. [Prerequisites](#prerequisites)
4. [Windows Setup](#windows-setup)
5. [Quick Start](#quick-start)
6. [Configuration](#configuration)
7. [Input File Format](#input-file-format)
8. [Ticker Validation](#ticker-validation)
9. [Pipeline Stages](#pipeline-stages)
10. [Transforms](#transforms)
11. [Running Tests](#running-tests)
12. [Output Layout](#output-layout)
13. [Runbook](#runbook)
14. [Extending the Pipeline](#extending-the-pipeline)

---

## Project Structure

```
stockanalyser/
│
├── src/
│   └── stockanalyser/
│       ├── __init__.py
│       ├── spark_session.py          # Spark session factory (handles Windows config)
│       ├── config/
│       │   ├── __init__.py
│       │   └── settings.py           # Centralised config (reads local.env)
│       ├── ingestion/
│       │   ├── __init__.py
│       │   ├── polygon_client.py     # HTTP client for Polygon REST API
│       │   ├── ticker_validator.py   # Pre-flight ticker validation
│       │   └── ingestor.py           # Reads CSV + validates + fetches all tickers
│       ├── schemas/
│       │   ├── __init__.py
│       │   └── stock_schema.py       # All StructType schemas (source of truth)
│       ├── validation/
│       │   ├── __init__.py
│       │   └── validators.py         # Data-quality gate (valid / quarantine)
│       └── transforms/
│           ├── __init__.py
│           ├── relative_increase.py  # T1 – greatest relative price increase
│           ├── portfolio.py          # T2 – $1M portfolio simulation
│           ├── monthly_cagr.py       # T3 – monthly CAGR Jan–June
│           └── weekly_decrease.py    # T4 – greatest single-week decrease
│
├── jobs/
│   └── run_pipeline.py               # Main entry-point (wires all stages)
│
├── data/
│   ├── stocks_list.csv               # S&P 500 top 100 company list (corrected)
│   └── stocks_list_test.csv          # 2-stock test file (AAPL + MSFT)
│
├── output/                           # Created at runtime
│   ├── raw/                          # Raw Parquet (partitioned by symbol)
│   ├── quarantine/                   # Bad rows with failure reason
│   ├── t1_relative_increase/
│   ├── t2_portfolio_detail/
│   ├── t3_monthly_cagr/
│   └── t4_weekly_decrease/
│
├── tests/
│   ├── conftest.py                   # Shared Spark session + data fixtures
│   ├── unit/
│   │   ├── test_schemas.py
│   │   ├── test_validators.py
│   │   ├── test_transforms.py
│   │   ├── test_ingestion.py
│   │   └── test_ticker_validator.py
│   └── integration/
│       └── test_pipeline_integration.py
│
├── diagnostics/
│   ├── inspect_data.py               # Inspect raw Parquet data for a symbol
│   └── debug_ticker.py               # Inspect Polygon reference response for a ticker
├── setup_windows.py                  # One-time Windows setup (downloads winutils.exe)
├── local.env                         # Secrets & runtime config
├── .gitignore
├── setup.py
├── pytest.ini
└── requirements.txt
```

---

## Architecture Overview

```
CSV Input
    │
    ▼
┌──────────────────────┐
│   TickerValidator    │  ← pre-flight: validates every symbol before any
│   (pre-ingestion)    │    data is fetched. Rejects invalid, inactive, non-stock
└────────┬─────────────┘    or name-mismatched tickers. See Ticker Validation below.
         │ valid tickers only
         ▼
┌──────────────────────┐
│  StockDataIngestor   │  ← fetches adjusted daily bars from Polygon API
│  (ingestion layer)   │
└────────┬─────────────┘
         │ RAW DataFrame
         ▼
┌──────────────────────┐
│  StockDataValidator  │  ← enforces data contracts, splits valid/quarantine
│  (validation layer)  │
└────────┬─────────────┘
         │ VALIDATED DataFrame
         ▼
┌────────────────────────────────────────────┐
│              Transform Layer               │
│  T1: Relative Increase                     │
│  T2: Portfolio Simulation                  │
│  T3: Monthly CAGR (Jan–June)               │
│  T4: Greatest Weekly Decrease              │
└────────────────────────────────────────────┘
         │
         ▼
    Parquet Output
```

**Data flow follows the Medallion pattern:**
- **Raw** – data exactly as received from the API (date as string)
- **Validated** – type-cast, null-checked, price-range checked (date as DateType)
- **Enriched** – transform outputs ready for downstream consumption

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.10+ | |
| Java | 11 or 17 | Required by PySpark |
| pip | Latest | |

---

## Windows Setup

PySpark on Windows requires `winutils.exe` to read and write files. Run this **once** before anything else:

```bat
python setup_windows.py
```

This downloads `winutils.exe` and `hadoop.dll` into `hadoop/bin/`. The `spark_session.py` factory automatically picks this up on every subsequent run — no manual environment variables needed.

---

## Quick Start

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd stockanalyser

# 2. Windows only — download winutils
python setup_windows.py

# 3. Create a virtual environment.. A virtual environment is used for the purpose of this assessment to isolate the project's dependencies from your global Python installation, ensuring consistent and reproducible behaviour.
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

# 4. Install dependencies
pip install -r requirements.txt
pip install -e .


# 5. Create your local.env from the example and set your API key
cp local.env.example local.env
# Then open local.env and set API_KEY=<your Polygon.io API key>

# 6. Run the pipeline
python jobs/run_pipeline.py --input data/stocks_list.csv
```

---

## Configuration

All configuration lives in `local.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | *(required)* | Polygon.io API key |
| `API_BASE_URL` | `https://api.polygon.io` | API base URL |
| `START_DATE` | `2025-01-01` | Analysis start date |
| `END_DATE` | `2025-12-31` | Analysis end date |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `OUTPUT_DIR` | `output` | Root output directory |
| `INPUT_FILE` | `data/stocks_list.csv` | Company list path |
| `TOTAL_INVESTMENT` | `1000000` | Portfolio total capital |
| `INVESTMENT_PER_STOCK` | `10000` | Per-stock budget |
| `API_REQUEST_DELAY` | `12` | Seconds between requests (12s = free tier) |
| `API_MAX_WORKERS` | `1` | Concurrent fetch threads |
| `API_MAX_RETRIES` | `3` | HTTP retry attempts |
| `API_RETRY_BACKOFF` | `1.5` | Exponential backoff factor |

**API rate limiting by Polygon tier:**

| Tier | `API_REQUEST_DELAY` | `API_MAX_WORKERS` | Est. time for 100 stocks |
|------|--------------------|--------------------|--------------------------|
| Free (5 req/min) | `12` | `1` | ~20 minutes |

---

## Input File Format

The input CSV must have exactly these two columns:

```
company_name,symbol
Apple Inc.,AAPL
Microsoft Corporation,MSFT
```

The `company_name` column is used during ticker validation (see below) to verify that the symbol actually belongs to the expected company. Both columns are required.

### Known Issues with Provided Input Files

When sourcing S&P 500 constituent lists from third parties, be aware that **tickers change over time** as companies rebrand or restructure. The corrected file `data/stocks_list.csv` has already been validated and fixed. If you supply your own list, the pipeline will validate it automatically and report any problems before fetching data.

Two specific issues were found and corrected in the original input file provided with this assessment:

| Original | Problem | Corrected |
|----------|---------|-----------|
| `FB` | Meta Platforms changed its ticker to `META` in 2022. The `FB` ticker was subsequently reused by an unrelated ETF (ProShares S&P 500 Dynamic Buffer ETF). | `META` |
| `ANTM` | Anthem Inc. rebranded to Elevance Health in 2022 and changed its ticker to `ELV`. `ANTM` is now inactive on Polygon. | `ELV` |

---

## Ticker Validation

Before any bar data is fetched from the API, every ticker in the input file is validated against the Polygon reference endpoint. This runs automatically as part of every full ingestion run.

**Checks performed:**

1. **Format** — symbol must be 1–5 uppercase letters, optionally followed by a dot and 1–2 letters (e.g. `BRK.B`)
2. **Duplicates** — duplicate symbols in the input file are warned and deduplicated
3. **Active status** — inactive or delisted tickers are rejected
4. **Type check** — Polygon must return type `CS` (common stock). ETFs, funds, bonds, warrants and other instrument types are rejected. This catches cases where an old ticker has been recycled by a different financial instrument (e.g. `FB` becoming an ETF)
5. **Name match** — the company name in the CSV is compared against the name Polygon returns for that ticker. If there is no meaningful word overlap the ticker is rejected as a probable reuse by a different company

**Example validation report:**

```
============================================================
 TICKER VALIDATION REPORT
============================================================
 Total tickers   : 100
 Valid           : 98
 Invalid/Unknown : 2
 Warnings        : 0

 INVALID TICKERS (will be skipped):
   ✗  FB         'ProShares S&P 500 Dynamic Buffer ETF' is not a stock
                 (Polygon type: 'ETF')
   ✗  ANTM       Ticker is inactive/delisted (was: 'Anthem Inc.')
============================================================
```

Invalid tickers are skipped and the pipeline continues with the remaining valid tickers. If **all** tickers fail validation the pipeline aborts.

**To debug a specific ticker manually:**

```bat
python diagnostics/debug_ticker.py FB
```

This prints the raw Polygon reference response so you can see exactly what the API returns for that symbol.

---

## Pipeline Stages

### Stage 1 – Ticker Validation + Ingestion

`TickerValidator` runs first, checking every symbol in the input file before any bar data is fetched. Invalid tickers are reported and skipped. `StockDataIngestor` then fetches adjusted daily OHLCV bars from Polygon for each valid ticker and persists the raw data as Parquet partitioned by `symbol`.

**Polygon endpoints used:**
```
# Reference / validation
GET /v3/reference/tickers/{ticker}
Authorization: Bearer {API_KEY}

# Daily bars
GET /v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}
    ?adjusted=true&sort=asc&limit=50000
Authorization: Bearer {API_KEY}
```

Note: `adjusted=true` is used so that corporate actions such as stock splits are reflected in historical prices. This prevents artificial price drops from appearing as genuine weekly losses.

### Stage 2 – Validation

`StockDataValidator` applies 11 named quality rules to the raw bar data. Rows failing any rule are routed to a `quarantine` output with the first failing rule name attached as `_failure_reason`. Rules include:

- Non-null symbol / date / OHLC prices
- Close price > $0.01
- High ≥ Low, High ≥ Close, Low ≤ Close
- Non-negative volume
- Valid date format

### Stage 3 – Transforms

See [Transforms](#transforms) below.

### Stage 4 – Output

Each transform writes its result as Parquet under `output/`. The raw and quarantine layers are also persisted.

---

## Transforms

### T1 – Greatest Relative Price Increase

**Question:** Which stock had the greatest relative increase in price over the period?

**Formula:**
```
relative_increase = (end_price - start_price) / start_price
```

Where `start_price` is the close on the **first available trading day** and `end_price` is the close on the **last available trading day** within the configured date range.

Output is ranked descending; Rank 1 = greatest increase.

---

### T2 – Portfolio Simulation ($1M)

**Question:** If you invested $1,000,000 by allocating $10,000 to every company, how much would you have today?

**Rules:**
1. Allocate $10,000 per stock.
2. Buy `floor($10,000 / start_price)` whole shares per stock. No fractional shares.
3. Collect the fractional remainder from all stocks into a **cash pool**.
4. **Redistribute the pool** greedily: sort stocks by price (cheapest first), buy additional whole shares while the pool covers them. This maximises capital deployed.
5. Final value = shares × last available close price.
6. Dividends are **ignored** per the brief.

A small residual cash amount (typically under $20) will remain uninvested — this is the irreducible minimum after exhausting all whole-share purchase opportunities across all stocks.

---

### T3 – Monthly CAGR (January to June)

**Question:** Which stock had the greatest monthly CAGR between January and June?

**Formula** (same structure as annual CAGR, months as periods):
```
monthly_CAGR = (end_price / start_price) ^ (1 / n_months) - 1
```

Where:
- `start_price` = close on the last trading day of **January**
- `end_price` = close on the last trading day of **June**
- `n_months` = 5 (January-end → February-end → … → June-end = 5 steps)

Stocks missing January or June data are excluded.

---

### T4 – Greatest Weekly Decrease

**Question:** Which stock had the greatest decrease within a single week, and which week?

**Definition:**
- Weeks are identified by their **Monday date** using `date_trunc("week")`. This avoids a Spark issue where `weekofyear` and `year` disagree at year-end boundaries (e.g. Dec 29–31 have `weekofyear=1` but `year=<previous year>`), which would otherwise create fake weeks spanning the entire year.
- `week_start_price` = close on the **first trading day** of that week.
- `week_end_price` = close on the **last trading day** of that week.
- Weeks with fewer than 2 trading days are excluded.
- `weekly_return = (week_end - week_start) / week_start`

The single (stock, week) combination with the most negative return is returned.

---

## Running Tests

```bash
# Run all tests
pytest

# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# With coverage report
pytest --cov=stockanalyser --cov-report=html

# Specific test file
pytest tests/unit/test_ticker_validator.py -v
```
## Output Layout

After a successful run:

```
output/
├── raw/
│   ├── symbol=AAPL/part-00000.parquet
│   ├── symbol=MSFT/part-00000.parquet
│   └── ...
├── quarantine/          (only present if bad rows found)
│   └── part-00000.parquet
├── t1_relative_increase/
│   └── part-00000.parquet
├── t2_portfolio_detail/
│   └── part-00000.parquet
├── t3_monthly_cagr/
│   └── part-00000.parquet
└── t4_weekly_decrease/
    └── part-00000.parquet
```

Read any output back with Spark:
```python
spark.read.parquet("output/t1_relative_increase").show()
```

---

## Runbook

### First time setup (Windows)

```bat
python setup_windows.py
```

## Running the full pipeline
```bat
# Normal run — no ticker validation, ~20 mins
python jobs/run_pipeline.py --input data/stocks_list.csv

# With ticker validation — ~40 mins, use when input file is new or changed
python jobs/run_pipeline.py --input data/stocks_list.csv --validate-tickers

# Skip ingestion — reuse already-saved raw data and run transforms only
python jobs/run_pipeline.py --skip-ingestion

# Quick test with 4 stocks
python jobs/run_pipeline.py --input data/stocks_list_test.csv --no-validate
```

```bat
# Inspect raw data for a specific symbol and date range
python diagnostics/inspect_data.py --symbol UNH --from 2025-05-12 --to 2025-05-16

## Check what Polygon returns for a ticker (useful for debugging validation failures)

# Current state (today)
python diagnostics/debug_ticker.py MMC

# As of the analysis start date
python diagnostics/debug_ticker.py MMC --date 2025-01-02

# Useful for comparing before/after a rebrand
python diagnostics/debug_ticker.py MMC --date 2026-01-01

```

### Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `ValidationError: api_key` | `local.env` not found or API_KEY not set | Ensure `local.env` exists in the project root and `API_KEY` is set |
| `HTTP 403 Forbidden` | Invalid API key | Verify your Polygon.io key is active |
| `HTTP 429 Too Many Requests` | Rate limit hit | Increase `API_REQUEST_DELAY` in `local.env` |
| `HADOOP_HOME unset` error | winutils not downloaded | Run `python setup_windows.py` |
| `Python worker failed to connect` | Wrong Python on PATH | Already handled by `spark_session.py` — ensure you're in your venv |
| `ModuleNotFoundError: stockanalyser` | Package not installed | Run `pip install -e .` |
| Tickers missing from results | Validation rejected them | Check the validation report printed at ingestion start |
| Suspicious transform results | Bad data or wrong ticker | Run `python diagnostics/inspect_data.py --symbol <SYM>` to inspect raw prices |

### Re-running after a partial failure

The pipeline writes each stage with `mode("overwrite")` so re-running is safe and idempotent. If the pipeline fails after ingestion but before transforms complete, use `--skip-ingestion` to avoid re-fetching all data.

---

## Extending the Pipeline

### Adding a new transform

1. Create `src/stockanalyser/transforms/my_transform.py` with a `compute_my_transform(df) -> DataFrame` function.
2. Add the output schema to `schemas/stock_schema.py`.
3. Import and call the function in `jobs/run_pipeline.py`.
4. Add tests in `tests/unit/test_transforms.py`.

### Adding a new ticker validation rule

Add a new check inside `_check_one()` in `ticker_validator.py`. Return `{"valid": False, "reason": "..."}` to reject or `{"valid": True}` to pass.

### Switching to a cloud Spark cluster

Update `local.env`:
```
SPARK_MASTER=spark://my-cluster:7077
```
Or set `SPARK_MASTER=yarn` for YARN clusters. Parquet output paths should be updated to `s3a://`, `gs://`, or `abfss://` as appropriate.
