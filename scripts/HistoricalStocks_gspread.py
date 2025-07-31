import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from time import sleep
import time
import sys
import os 

# === Setup Google Sheets credentials ===
scope = [
    "https://spreadsheets.google.com/feeds",    
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# === Google Sheet IDs ===
#TICKERS_SHEET_ID = " "
#HISTORICAL_SHEET_ID = " "

TICKERS_SHEET_ID = os.environ.get("TICKERS_SHEET_ID")
HISTORICAL_SHEET_ID = os.environ.get("HISTORICAL_SHEET_ID")

# === Load tickers from Google Sheets with retry ===
MAX_RETRIES = 5
for attempt in range(1, MAX_RETRIES + 1):
    try:
        print(f"[INFO] Loading tickers from Google Sheets (Attempt {attempt})...")
        ticker_sheet = client.open_by_key(TICKERS_SHEET_ID).sheet1
        tickers_df = pd.DataFrame(ticker_sheet.get_all_records())
        break
    except Exception as e:
        print(f"[ERROR] Failed to read tickers (Attempt {attempt}/{MAX_RETRIES}): {e}")
        if attempt == MAX_RETRIES:
            sys.exit(1)
        time.sleep(5 * attempt)

tickers = tickers_df["ticker"].dropna().tolist()
tickers = [t.replace('.', '-') for t in tickers]

# === Filter out tickers with no data ===
valid_tickers = []
for t in tickers:
    try:
        if not yf.Ticker(t).history(period="1d").empty:
            valid_tickers.append(t)
        else:
            print(f"[WARN] No price data for {t}. Skipping.")
    except Exception:
        print(f"[WARN] Failed to download {t}. Skipping.")

if not valid_tickers:
    print("No valid tickers found. Exiting.")
    sys.exit(0)

# === Download historical stock data ===
today = datetime.today().strftime('%Y-%m-%d')
df = yf.download(valid_tickers, start="2025-01-01", end=today, interval="1d", auto_adjust=True)['Close']

if df.empty:
    print("No data downloaded.")
    sys.exit(0)

df.index.name = "Date"
df = df[valid_tickers]
df.reset_index(inplace=True)

# === Remove dates that already exist in Google Sheet ===
try:
    sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
    existing_dates_raw = sheet.col_values(1)[1:]  # skip header
    existing_dates = set(pd.to_datetime(existing_dates_raw).strftime('%Y-%m-%d'))

    df['Date'] = pd.to_datetime(df['Date'])
    df['Date_str'] = df['Date'].dt.strftime('%Y-%m-%d')
    df = df[~df['Date_str'].isin(existing_dates)].drop(columns=["Date_str"])

    if df.empty:
        print("✅ No new data to upload. Skipping upload.")
        sys.exit(0)
    else:
        print(f"📈 {len(df)} new rows to upload.")
except Exception as e:
    print(f"[WARN] Could not read existing sheet dates. Will upload all data. Error: {e}")

# === Chunked upload helper ===
def update_sheet_in_chunks(sheet, df, chunk_size=500, max_retries=3):
    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    
    sheet_data = sheet.get_all_values()
    if not sheet_data or sheet_data[0] != df.columns.tolist():
        sheet.clear()
        sheet.update(values=[df.columns.tolist()], range_name="A1")

    start_row = len(sheet.get_all_values()) + 1
    for i in range(0, len(data) - 1, chunk_size):  # skip header
        chunk = data[i + 1:i + 1 + chunk_size]
        cell_range = f"A{start_row + i}"
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[UPLOAD] Uploading rows {start_row + i} to {start_row + i + len(chunk) - 1}...")
                sheet.update(values=chunk, range_name=cell_range)
                sleep(1)
                break
            except Exception as e:
                print(f"❌ Failed to upload chunk (Attempt {attempt}):", e)
                if attempt == max_retries:
                    raise
                sleep(2)

# === Final Upload ===
try:
    update_sheet_in_chunks(sheet, df)
    print("✅ Upload to Google Sheets successful.")
except Exception as e:
    print("❌ Failed to upload to Google Sheets:", e)
    fallback_path = "Historical_Stocks.csv"
    df.to_csv(fallback_path, index=False)
    print(f"[📄] Fallback saved to {fallback_path}")
