import pandas as pd
import yfinance as yf
from datetime import datetime
import os

# === Define paths ===
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)

TICKERS_FILE = os.path.join(RAW_DIR, "Tickers_Codes.csv")
STOCK_INFO_FILE = os.path.join(PROCESSED_DIR, "Stock_Info.csv")

print(f"[INFO] Starting stock info retrieval at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# === Load and clean tickers ===
try:
    tickers_df = pd.read_csv(TICKERS_FILE)
    tickers = tickers_df["ticker"].dropna().tolist()
    tickers = [ticker.replace('.', '-') for ticker in tickers]  # Fix for yfinance
    tickers_dict = {str(ticker): True for ticker in tickers}
    print(f"[INFO] Loaded {len(tickers)} tickers.")
except Exception as e:
    print(f"[ERROR] Failed to load tickers from {TICKERS_FILE}: {e}")
    exit()

# === Fetch financial info for each ticker ===
dow_stats = {}

for ticker in tickers_dict:
    ticker_yf = yf.Ticker(ticker)

    try:
        info = ticker_yf.get_info()
        if not info:
            print(f"[WARN] No info returned for {ticker}")
            continue
    except Exception as e:
        print(f"[ERROR] Failed to get info for {ticker}: {e}")
        continue

    attributes_of_interest = [
        "marketCap", "trailingPE", "forwardPE", "beta", "trailingEps",
        "industry", "sector", "fullTimeEmployees", "country", "ebitda",
        "totalDebt", "totalRevenue", "grossProfits", "freeCashflow",
        "shortName", "trailingPegRatio"
    ]

    filtered_data = {attr: info.get(attr) for attr in attributes_of_interest}
    ticker_info = pd.DataFrame([filtered_data])
    dow_stats[ticker] = ticker_info

# === Save financial stats ===
if dow_stats:
    all_stats_info = pd.concat(dow_stats, keys=dow_stats.keys(), names=['ticker', 'Index'])
    all_stats_info.to_csv(STOCK_INFO_FILE)
    print(f"[INFO] Financial info saved to: {STOCK_INFO_FILE}")
else:
    print("[WARN] No financial data was saved.")
