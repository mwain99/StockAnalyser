"""
debug_ticker.py
---------------
Shows exactly what Polygon returns for a ticker symbol.
Optionally pass a date to see what the ticker looked like on that date.

Usage:
    python diagnostics/debug_ticker.py FB
    python diagnostics/debug_ticker.py MMC --date 2025-01-02
    python diagnostics/debug_ticker.py MMC --date 2026-01-01
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import os
from dotenv import load_dotenv

env_file = Path(__file__).resolve().parents[1] / "local.env"

if not env_file.exists():
    print(f"ERROR: local.env not found at {env_file}")
    sys.exit(1)

load_dotenv(dotenv_path=env_file, override=True)

api_key = os.environ.get("API_KEY")

if not api_key or api_key == "your_polygon_api_key_here":
    print(f"ERROR: API_KEY not set in {env_file}")
    sys.exit(1)

import requests

parser = argparse.ArgumentParser()
parser.add_argument("ticker", help="Ticker symbol to look up")
parser.add_argument(
    "--date",
    default=None,
    help="Check ticker as of this date (YYYY-MM-DD). Defaults to today (current state).",
)
args = parser.parse_args()

url = f"https://api.polygon.io/v3/reference/tickers/{args.ticker.upper()}"
if args.date:
    url += f"?date={args.date}"

print(f"URL    : {url}")
resp = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10)

print(f"Status : {resp.status_code}")
print(f"Body   : {resp.json()}")