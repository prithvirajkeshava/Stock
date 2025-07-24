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
CLOSING_PRICE_FILE = os.path.join(PROCESSED_DIR, "Main_Actual_Stock.csv")

print(f"[INFO] Starting data retrieval at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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

# === Download latest closing prices ===
df = yf.download(tickers, period="5d", interval="1d", auto_adjust=True)['Close']
if df.empty:
    print("[ERROR] No data downloaded — possible market closure or API issue.")
    exit()

df = df.tail(1)
df.index = pd.to_datetime(df.index).normalize()
new_date = df.index[0]
print(f"[INFO] Latest data retrieved for: {new_date.date()}")

# === Initialize data holder ===
dow_stats = {}

# === Fetch financial info for each ticker ===
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

# === Save closing prices ===
if not os.path.exists(CLOSING_PRICE_FILE) or os.stat(CLOSING_PRICE_FILE).st_size == 0:
    df.to_csv(CLOSING_PRICE_FILE, index=True)
    print(f"[INFO] Created new file: {CLOSING_PRICE_FILE}")
else:
    try:
        df_existing = pd.read_csv(CLOSING_PRICE_FILE, index_col="Date")
        df_existing.index = pd.to_datetime(df_existing.index).normalize()

        if new_date not in df_existing.index:
            df_final = pd.concat([df_existing, df])
            df_final.to_csv(CLOSING_PRICE_FILE)
            print(f"[INFO] Closing prices updated with new date: {new_date.date()}")
        else:
            print(f"[INFO] Date {new_date.date()} already exists — no update needed.")
    except Exception as e:
        print(f"[ERROR] Failed to load or update existing closing prices: {e}")
