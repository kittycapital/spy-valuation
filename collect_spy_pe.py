#!/usr/bin/env python3
"""
SPY Valuation Dashboard - Data Collector
Fetches Shiller CAPE/P/E data + SPY prices and builds JSON for the dashboard.

Usage:
  python collect_spy_pe.py --full    # Initial: Shiller Excel + SPY CSV
  python collect_spy_pe.py --update  # Daily: Yahoo Finance SPY price only
  python collect_spy_pe.py --repair  # Re-download Shiller + rebuild
"""

import json
import csv
import sys
import os
import time
import struct
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

DATA_DIR = Path("data")

SHILLER_URL = "http://www.econ.yale.edu/~shiller/data/ie_data.xls"

# Valuation bands for CAPE
CAPE_BANDS = [
    {"label": "매우 저평가", "min": 0, "max": 15, "color": "rgba(34,197,94,0.08)", "border": "rgba(34,197,94,0.3)"},
    {"label": "저평가", "min": 15, "max": 20, "color": "rgba(59,130,246,0.06)", "border": "rgba(59,130,246,0.2)"},
    {"label": "적정", "min": 20, "max": 25, "color": "rgba(107,114,128,0.04)", "border": "rgba(107,114,128,0.15)"},
    {"label": "고평가", "min": 25, "max": 30, "color": "rgba(245,158,11,0.06)", "border": "rgba(245,158,11,0.2)"},
    {"label": "과열", "min": 30, "max": 999, "color": "rgba(239,68,68,0.06)", "border": "rgba(239,68,68,0.2)"},
]

# Valuation bands for Trailing P/E
PE_BANDS = [
    {"label": "매우 저평가", "min": 0, "max": 12, "color": "rgba(34,197,94,0.08)", "border": "rgba(34,197,94,0.3)"},
    {"label": "저평가", "min": 12, "max": 15, "color": "rgba(59,130,246,0.06)", "border": "rgba(59,130,246,0.2)"},
    {"label": "적정", "min": 15, "max": 20, "color": "rgba(107,114,128,0.04)", "border": "rgba(107,114,128,0.15)"},
    {"label": "고평가", "min": 20, "max": 25, "color": "rgba(245,158,11,0.06)", "border": "rgba(245,158,11,0.2)"},
    {"label": "과열", "min": 25, "max": 999, "color": "rgba(239,68,68,0.06)", "border": "rgba(239,68,68,0.2)"},
]


def download_file(url, dest):
    """Download a file from URL."""
    print(f"  Downloading {url}...")
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urlopen(req, timeout=60)
        with open(dest, "wb") as f:
            f.write(resp.read())
        print(f"  Saved to {dest} ({dest.stat().st_size / 1024:.1f} KB)")
        return True
    except Exception as e:
        print(f"  Download failed: {e}")
        return False


def parse_xls_basic(filepath):
    """
    Parse old-format .xls (BIFF8) to extract Shiller data.
    Tries xlrd first, then libreoffice+openpyxl fallback.
    """
    # Try xlrd (works in GitHub Actions)
    try:
        import xlrd
        return parse_xls_xlrd(filepath)
    except ImportError:
        pass

    # Try libreoffice conversion (works locally)
    import subprocess
    xlsx_path = filepath.with_suffix(".xlsx")
    try:
        subprocess.run(
            ["libreoffice", "--headless", "--convert-to", "xlsx",
             "--outdir", str(filepath.parent), str(filepath)],
            capture_output=True, timeout=60
        )
        if xlsx_path.exists():
            return parse_xlsx(xlsx_path)
    except Exception as e:
        print(f"  LibreOffice conversion failed: {e}")

    # Fallback: existing CSV
    csv_fallback = DATA_DIR / "shiller_data.csv"
    if csv_fallback.exists():
        print("  Using existing shiller_data.csv")
        return parse_shiller_csv(csv_fallback)

    return None


def parse_xls_xlrd(filepath):
    """Parse xls file using xlrd."""
    import xlrd
    wb = xlrd.open_workbook(str(filepath))
    ws = wb.sheet_by_name("Data")

    data = []
    for r in range(8, ws.nrows):
        date_val = ws.cell_value(r, 0)
        price = ws.cell_value(r, 1)
        earnings = ws.cell_value(r, 3)
        cape = ws.cell_value(r, 12)

        if not date_val or not price:
            continue
        if not isinstance(date_val, (int, float)):
            continue

        year = int(date_val)
        month = round((date_val - year) * 100)
        if month == 0:
            month = 1
        date_str = f"{year}-{month:02d}"

        trailing_pe = None
        if isinstance(earnings, (int, float)) and earnings > 0:
            trailing_pe = round(price / earnings, 2)

        cape_val = None
        if isinstance(cape, (int, float)):
            cape_val = round(cape, 2)

        data.append({
            "date": date_str,
            "sp500": round(price, 2),
            "earnings": round(earnings, 4) if isinstance(earnings, (int, float)) else None,
            "cape": cape_val,
            "trailing_pe": trailing_pe,
        })

    print(f"  Parsed {len(data)} monthly records from Shiller data (xlrd)")
    return data


def parse_xlsx(filepath):
    """Parse xlsx file using openpyxl."""
    try:
        import openpyxl
    except ImportError:
        print("  openpyxl not available, trying alternative...")
        return None

    wb = openpyxl.load_workbook(str(filepath), data_only=True)
    ws = wb["Data"]

    # Find header row (row 8 has column names)
    # Date, P, D, E, CPI, Fraction, Rate GS10, Real Price, Real Dividend,
    # Real Return Price, Real Earnings, Scaled Earnings, CAPE, _, TR CAPE
    data = []
    for r in range(9, ws.max_row + 1):
        date_val = ws.cell(r, 1).value
        price = ws.cell(r, 2).value
        earnings = ws.cell(r, 4).value
        cape = ws.cell(r, 13).value

        if date_val is None or price is None:
            continue

        # Convert date like 2023.09 to "2023-09"
        year = int(date_val)
        month = round((date_val - year) * 100)
        if month == 0:
            month = 1
        date_str = f"{year}-{month:02d}"

        # Calculate trailing P/E
        trailing_pe = None
        if earnings and earnings > 0:
            trailing_pe = round(price / earnings, 2)

        # CAPE
        cape_val = None
        if cape and cape != "NA" and isinstance(cape, (int, float)):
            cape_val = round(cape, 2)

        data.append({
            "date": date_str,
            "sp500": round(price, 2),
            "earnings": round(earnings, 4) if earnings else None,
            "cape": cape_val,
            "trailing_pe": trailing_pe,
        })

    print(f"  Parsed {len(data)} monthly records from Shiller data")
    return data


def parse_shiller_csv(filepath):
    """Parse pre-converted CSV of Shiller data."""
    data = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                data.append({
                    "date": row["date"],
                    "sp500": float(row["sp500"]) if row.get("sp500") else None,
                    "earnings": float(row["earnings"]) if row.get("earnings") else None,
                    "cape": float(row["cape"]) if row.get("cape") else None,
                    "trailing_pe": float(row["trailing_pe"]) if row.get("trailing_pe") else None,
                })
            except (ValueError, KeyError):
                continue
    print(f"  Parsed {len(data)} records from CSV")
    return data


def load_spy_csv():
    """Load SPY daily prices from CSV."""
    csv_path = DATA_DIR / "SPY.csv"
    if not csv_path.exists():
        csv_path = Path("SPY.csv")
    if not csv_path.exists():
        print("  SPY.csv not found")
        return {}

    prices = {}
    with open(csv_path, "r") as f:
        header = True
        for line in f:
            if header:
                header = False
                continue
            parts = line.strip().split(",")
            if len(parts) >= 2:
                try:
                    date_str = parts[0].strip()
                    close = float(parts[1])
                    prices[date_str] = round(close, 2)
                except (ValueError, IndexError):
                    continue
    print(f"  Loaded {len(prices)} daily SPY prices from CSV")
    return prices


def fetch_spy_today():
    """Fetch latest SPY price from Yahoo Finance."""
    print("  Fetching latest SPY price from Yahoo Finance...")
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/SPY?range=5d&interval=1d"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urlopen(req, timeout=15)
        data = json.loads(resp.read())

        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]

        prices = {}
        for ts, close in zip(timestamps, closes):
            if close is not None:
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                prices[date_str] = round(close, 2)

        print(f"  Got {len(prices)} recent SPY prices")
        return prices
    except Exception as e:
        print(f"  Failed to fetch SPY: {e}")
        return {}


def calculate_percentile(value, all_values):
    """Calculate historical percentile for a value."""
    if not all_values or value is None:
        return None
    below = sum(1 for v in all_values if v <= value)
    return round(below / len(all_values) * 100, 1)


def calculate_forward_returns(shiller_data, spy_daily):
    """
    Calculate average forward 10-year returns by CAPE range.
    Uses monthly S&P 500 data from Shiller.
    """
    # Build monthly price series
    monthly_prices = {}
    for entry in shiller_data:
        if entry["sp500"]:
            monthly_prices[entry["date"]] = entry["sp500"]

    sorted_dates = sorted(monthly_prices.keys())
    results = {}

    for i, date_str in enumerate(sorted_dates):
        cape = None
        for entry in shiller_data:
            if entry["date"] == date_str:
                cape = entry.get("cape")
                break

        if cape is None:
            continue

        # Find price 10 years later
        year = int(date_str[:4])
        month = int(date_str[5:7])
        future_date = f"{year + 10}-{month:02d}"

        if future_date in monthly_prices:
            start_price = monthly_prices[date_str]
            end_price = monthly_prices[future_date]
            annual_return = ((end_price / start_price) ** (1 / 10) - 1) * 100

            # Categorize by CAPE band
            band_key = None
            if cape < 15:
                band_key = "<15"
            elif cape < 20:
                band_key = "15-20"
            elif cape < 25:
                band_key = "20-25"
            elif cape < 30:
                band_key = "25-30"
            else:
                band_key = "30+"

            if band_key not in results:
                results[band_key] = []
            results[band_key].append(annual_return)

    # Calculate averages
    summary = {}
    for band, returns in results.items():
        summary[band] = {
            "avg": round(sum(returns) / len(returns), 1),
            "min": round(min(returns), 1),
            "max": round(max(returns), 1),
            "count": len(returns),
        }

    return summary


def build_chart_data(shiller_data, spy_daily):
    """Build the final JSON data for the dashboard."""

    # All CAPE values for percentile
    all_capes = [e["cape"] for e in shiller_data if e["cape"] is not None]
    all_pes = [e["trailing_pe"] for e in shiller_data if e["trailing_pe"] is not None]

    # Latest values
    latest_shiller = None
    for e in reversed(shiller_data):
        if e["cape"] is not None:
            latest_shiller = e
            break

    latest_pe_entry = None
    for e in reversed(shiller_data):
        if e["trailing_pe"] is not None:
            latest_pe_entry = e
            break

    # Latest SPY price
    spy_sorted = sorted(spy_daily.keys())
    latest_spy_price = spy_daily[spy_sorted[-1]] if spy_sorted else None
    latest_spy_date = spy_sorted[-1] if spy_sorted else None

    # Forward returns
    forward_returns = calculate_forward_returns(shiller_data, spy_daily)

    chart_data = {
        "shiller": [],
        "spy_daily": {},
        "summary": {
            "latest_cape": latest_shiller["cape"] if latest_shiller else None,
            "latest_cape_date": latest_shiller["date"] if latest_shiller else None,
            "cape_percentile": calculate_percentile(
                latest_shiller["cape"] if latest_shiller else None, all_capes
            ),
            "latest_pe": latest_pe_entry["trailing_pe"] if latest_pe_entry else None,
            "latest_pe_date": latest_pe_entry["date"] if latest_pe_entry else None,
            "pe_percentile": calculate_percentile(
                latest_pe_entry["trailing_pe"] if latest_pe_entry else None, all_pes
            ),
            "latest_spy": latest_spy_price,
            "latest_spy_date": latest_spy_date,
            "cape_median": round(sorted(all_capes)[len(all_capes) // 2], 1) if all_capes else None,
            "cape_mean": round(sum(all_capes) / len(all_capes), 1) if all_capes else None,
            "pe_median": round(sorted(all_pes)[len(all_pes) // 2], 1) if all_pes else None,
            "pe_mean": round(sum(all_pes) / len(all_pes), 1) if all_pes else None,
        },
        "forward_returns": forward_returns,
        "cape_bands": CAPE_BANDS,
        "pe_bands": PE_BANDS,
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "shiller_records": len(shiller_data),
            "spy_daily_records": len(spy_daily),
        },
    }

    # Trim shiller data to 1900+ for chart (pre-1900 is less relevant)
    for entry in shiller_data:
        if entry["date"] >= "1900":
            chart_data["shiller"].append(entry)

    # SPY daily (only keep monthly samples for reasonable JSON size)
    for date_str in sorted(spy_daily.keys()):
        chart_data["spy_daily"][date_str] = spy_daily[date_str]

    return chart_data


def save_shiller_csv(shiller_data):
    """Save parsed Shiller data as CSV for future fallback."""
    csv_path = DATA_DIR / "shiller_data.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "sp500", "earnings", "cape", "trailing_pe"])
        writer.writeheader()
        writer.writerows(shiller_data)
    print(f"  Saved Shiller CSV backup: {csv_path}")


def run_full():
    """Full data collection: Shiller Excel + SPY CSV."""
    DATA_DIR.mkdir(exist_ok=True)

    # 1. Download and parse Shiller data
    print("\n=== Downloading Shiller CAPE Data ===")
    xls_path = DATA_DIR / "ie_data.xls"
    shiller_data = None

    # Try download first
    if download_file(SHILLER_URL, xls_path):
        shiller_data = parse_xls_basic(xls_path)

    # Fallback to local file in data/ or root
    if not shiller_data and xls_path.exists():
        print("  Using local file: " + str(xls_path))
        shiller_data = parse_xls_basic(xls_path)

    if not shiller_data:
        local_xls = Path("ie_data.xls")
        if local_xls.exists():
            shiller_data = parse_xls_basic(local_xls)

    if not shiller_data:
        print("ERROR: Cannot get Shiller data")
        sys.exit(1)

    save_shiller_csv(shiller_data)

    # 2. Load SPY prices
    print("\n=== Loading SPY Prices ===")
    spy_daily = load_spy_csv()
    today_prices = fetch_spy_today()
    spy_daily.update(today_prices)

    # 3. Build and save
    print("\n=== Building Chart Data ===")
    chart_data = build_chart_data(shiller_data, spy_daily)

    output_path = DATA_DIR / "spy_valuation.json"
    with open(output_path, "w") as f:
        json.dump(chart_data, f)
    print(f"\nData saved: {output_path} ({output_path.stat().st_size / 1024:.1f} KB)")


def run_update():
    """Daily update: just add latest SPY price."""
    data_path = DATA_DIR / "spy_valuation.json"
    if not data_path.exists():
        print("No existing data. Run --full first.")
        sys.exit(1)

    with open(data_path) as f:
        chart_data = json.load(f)

    # Fetch latest SPY prices
    today_prices = fetch_spy_today()
    if today_prices:
        chart_data["spy_daily"].update(today_prices)
        spy_sorted = sorted(chart_data["spy_daily"].keys())
        chart_data["summary"]["latest_spy"] = chart_data["spy_daily"][spy_sorted[-1]]
        chart_data["summary"]["latest_spy_date"] = spy_sorted[-1]

    chart_data["metadata"]["generated_at"] = datetime.now(timezone.utc).isoformat()
    chart_data["metadata"]["mode"] = "update"

    output_path = DATA_DIR / "spy_valuation.json"
    with open(output_path, "w") as f:
        json.dump(chart_data, f)
    print(f"Updated: {output_path}")


def run_repair():
    """Re-download Shiller data and rebuild everything."""
    run_full()


if __name__ == "__main__":
    if "--full" in sys.argv:
        run_full()
    elif "--update" in sys.argv:
        run_update()
    elif "--repair" in sys.argv:
        run_repair()
    else:
        print("Usage:")
        print("  python collect_spy_pe.py --full    # Initial full collection")
        print("  python collect_spy_pe.py --update  # Daily SPY price update")
        print("  python collect_spy_pe.py --repair  # Re-download Shiller + rebuild")
