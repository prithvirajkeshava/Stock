import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from time import sleep
import time
import sys

# === Setup Google Sheets credentials ===
scope = [
    "https://spreadsheets.google.com/feeds",    
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# === Google Sheet IDs ===
TICKERS_SHEET_ID = "1KCKBgqszZAoJk_RYndRvndP0CwE548LcLJOA6A_PCEQ" # insert from google drive folder
HISTORICAL_SHEET_ID = "15AbIBbwNGl6qThiZzxQuY6Dgk5UloEv5pUfIKlvGNDU" # insert from google drive folder

# === Load tickers from Google Sheets with retry ===
MAX_RETRIES = 5
for attempt in range(1, MAX_RETRIES + 1):
    try:
        print(f"[INFO] Loading tickers from Google Sheets (Attempt {attempt})...")
        ticker_sheet = client.open_by_key(TICKERS_SHEET_ID).sheet1
        tickers_df = pd.DataFrame(ticker_sheet.get_all_records())
        break  # Success
    except Exception as e:
        print(f"[ERROR] Failed to read tickers (Attempt {attempt}/{MAX_RETRIES}): {e}")
        if attempt == MAX_RETRIES:
            sys.exit(1)  # Fail if all retries exhausted
        time.sleep(5 * attempt)  # Exponential backoff

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
df = yf.download(valid_tickers, start="2015-01-02", end=today, interval="1d", auto_adjust=True)['Close']

if df.empty:
    print("No data downloaded.")
    sys.exit(0)

df.index.name = "Date"
df = df[valid_tickers]
df_combined = None

# === Try to load existing historical data from sheet ===
try:
    sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
    values = sheet.get_all_values()

    if not values or not values[0] or "Date" not in values[0]:
        print("[INFO] Sheet empty or malformed. Starting fresh.")
        df_combined = df
    else:
        existing_df = pd.DataFrame(values[1:], columns=values[0])
        existing_df.set_index("Date", inplace=True)
        existing_df.index = pd.to_datetime(existing_df.index)
        df.index = pd.to_datetime(df.index)

        if today not in existing_df.index.strftime('%Y-%m-%d'):
            df_combined = pd.concat([existing_df, df])
            print("[INFO] Appended new data.")
        else:
            df_combined = existing_df
            print("[INFO] Data for today already exists. No update.")
except Exception as e:
    print("[WARN] Sheet not found or error reading it:", e)
    df_combined = df

# === Chunked upload helper ===
def update_sheet_in_chunks(sheet, df, chunk_size=500, max_retries=3):
    data = [df.columns.tolist()] + df.astype(str).values.tolist()

    first_chunk = data[:chunk_size]
    try:
        print("[UPLOAD] Verifying upload with first chunk...")
        sheet.update(values=first_chunk, range_name="A1")
    except Exception as e:
        print("❌ First chunk upload failed. Aborting:", e)
        raise

    sheet.clear()
    sheet.update(values=first_chunk, range_name="A1")

    for i in range(chunk_size, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        cell_range = f"A{i+1}"
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[UPLOAD] Uploading rows {i+1} to {i+len(chunk)}...")
                sheet.update(values=chunk, range_name=cell_range)
                sleep(1)
                break
            except Exception as e:
                print(f"❌ Failed to upload chunk {i+1}-{i+len(chunk)} (Attempt {attempt}):", e)
                if attempt == max_retries:
                    raise
                sleep(2)

# === Final Upload ===
if df_combined is not None:
    df_to_write = df_combined.reset_index()
    try:
        sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
        update_sheet_in_chunks(sheet, df_to_write)
        print("✅ Upload to Google Sheets successful.")
    except Exception as e:
        print("❌ Failed to upload to Google Sheets:", e)
        fallback_path = "Historical_Stocks.csv"
        df_to_write.to_csv(fallback_path, index=False)
        print(f"[📄] Fallback saved to {fallback_path}")
