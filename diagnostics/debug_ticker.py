"""
debug_ticker.py - shows exactly what Polygon returns for a ticker
Usage: python debug_ticker.py FB
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import os
from dotenv import load_dotenv
load_dotenv("../local.env")

import requests

api_key = os.environ.get("API_KEY")
ticker = sys.argv[1] if len(sys.argv) > 1 else "FB"

url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
resp = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10)

print(f"Status : {resp.status_code}")
print(f"Body   : {resp.json()}")