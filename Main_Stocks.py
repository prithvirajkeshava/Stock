import pandas as pd
import yfinance as yf
from datetime import datetime
import os

# === Load and clean tickers ===
tickers_df = pd.read_csv("Tickers_Codes.csv")
tickers = tickers_df["ticker"].dropna().tolist()
tickers = [ticker.replace('.', '-') for ticker in tickers]  # Fix for yfinance
tickers_dict = {str(ticker): True for ticker in tickers}

# === Download latest closing prices ===
df = yf.download(tickers, period="5d", interval="1d", auto_adjust=True)['Close']
df = df.tail(1)

# === Initialize data holder ===
dow_stats = {}

# === Fetch financial info for each ticker ===
for i in tickers_dict:
    ticker_yf = yf.Ticker(i)
    
    try:
        temp = ticker_yf.get_info()
        if not temp:
            print(f"[WARN] No info returned for {i}")
            continue
    except Exception as e:
        print(f"[ERROR] Failed to get info for {i}: {e}")
        continue

    attributes_of_interest = [
        "marketCap", "trailingPE", "forwardPE", "beta", "trailingEps",
        "industry", "sector", "fullTimeEmployees", "country", "ebitda",
        "totalDebt", "totalRevenue", "grossProfits", "freeCashflow",
        "shortName", "trailingPegRatio"
    ]

    filtered_data = {attr: temp.get(attr) for attr in attributes_of_interest}
    ticker_info = pd.DataFrame([filtered_data])
    dow_stats[i] = ticker_info

# === Combine and save all stats ===
if dow_stats:
    all_stats_info = pd.concat(dow_stats, keys=dow_stats.keys(), names=['ticker', 'Index'])
    all_stats_info.to_csv("Stock_Info.csv")
else:
    print("[WARN] No financial data was saved.")

# === Save closing prices ===
file_name = "Main_Actual_Stock.csv"

if not os.path.exists(file_name) or os.stat(file_name).st_size == 0:
    df.to_csv(file_name, index=True)
    print("File created with initial data.")
else:
    df_existing = pd.read_csv(file_name, index_col="Date")
    df_existing.index = pd.to_datetime(df_existing.index)

    new_date = df.index[0]

    if new_date not in df_existing.index:
        df_final = pd.concat([df_existing, df])
        df_final.to_csv(file_name)
        print("Data updated.")
    else:
        print("Date already exists — no update needed.")
