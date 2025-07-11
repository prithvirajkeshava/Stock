import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

# === Setup Google Sheets credentials ===
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# === Google Sheet IDs ===
TICKERS_SHEET_ID = "1KCKBgqszZAoJk_RYndRvndP0CwE548LcLJOA6A_PCEQ"
HISTORICAL_SHEET_ID = "15AbIBbwNGl6qThiZzxQuY6Dgk5UloEv5pUfIKlvGNDU"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# === Load tickers from Google Sheets ===
ticker_sheet = client.open_by_key(TICKERS_SHEET_ID).sheet1
tickers_df = pd.DataFrame(ticker_sheet.get_all_records())
tickers = tickers_df["ticker"].dropna().tolist()
tickers = [t.replace('.', '-') for t in tickers]  # e.g., BRK.B → BRK-B

# === Filter out invalid tickers ===
valid_tickers = []
for t in tickers:
    try:
        if not yf.Ticker(t).history(period="1d").empty:
            valid_tickers.append(t)
        else:
            log(f"[WARN] No price data for {t}. Skipping.")
    except Exception:
        log(f"[WARN] Failed to download {t}. Skipping.")

if not valid_tickers:
    log("No valid tickers found.")
    exit()

# === Get today's date ===
today = datetime.today().strftime('%Y-%m-%d')

# === Download historical data ===
df = yf.download(valid_tickers, start="2015-01-02", end=today, interval="1d", auto_adjust=True)['Close']
if df.empty:
    log("No data downloaded.")
    exit()

df.index.name = "Date"
df = df[valid_tickers]
df_combined = None

# === Try to read existing sheet data ===
try:
    history_sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
    existing_data = history_sheet.get_all_values()

    if not existing_data or not existing_data[0] or "Date" not in existing_data[0]:
        log("Sheet empty or malformed. Starting fresh.")
        df_combined = df
    else:
        existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        existing_df.set_index("Date", inplace=True)
        existing_df.index = pd.to_datetime(existing_df.index)
        df.index = pd.to_datetime(df.index)

        if today not in existing_df.index.strftime('%Y-%m-%d'):
            df_combined = pd.concat([existing_df, df])
            log("✅ Appended new data.")
        else:
            df_combined = existing_df
            log("ℹ️ Today's data already exists. No update needed.")

except Exception as e:
    log(f"⚠️ Sheet not found or error reading it: {e}")
    df_combined = df

# === Upload to Google Sheets or fallback to local CSV ===
if df_combined is not None:
    df_to_write = df_combined.reset_index()
    columns = [str(c) for c in df_to_write.columns]
    rows = df_to_write.astype(str).values.tolist()

    try:
        history_sheet.clear()
        history_sheet.update([columns] + rows)
        log("✅ Sheet updated successfully.")
    except Exception as e:
        log(f"❌ Failed to upload to Google Sheets: {e}")
        fallback_path = "Historical_Stocks.csv"
        df_combined.to_csv(fallback_path)
        log(f"📄 Fallback saved to {fallback_path}")
