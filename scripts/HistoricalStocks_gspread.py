import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from time import sleep
import sys

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

# === Load tickers from Google Sheets ===
print("[INFO] Loading tickers from Google Sheets...")
try:
    ticker_sheet = client.open_by_key(TICKERS_SHEET_ID).sheet1
    tickers_df = pd.DataFrame(ticker_sheet.get_all_records())
    tickers = tickers_df["ticker"].dropna().tolist()
    tickers = [t.replace('.', '-') for t in tickers]  # e.g., BRK.B → BRK-B
    print(f"[INFO] Found {len(tickers)} tickers.")
except Exception as e:
    print(f"[ERROR] Failed to read tickers: {e}")
    sys.exit(1)

# === Filter valid tickers ===
print("[INFO] Validating tickers with available data...")
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
    print("[ERROR] No valid tickers found. Exiting.")
    sys.exit(1)

# === Read existing historical sheet data (if any) ===
print("[INFO] Checking existing historical data...")
try:
    sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
    values = sheet.get_all_values()

    if not values or not values[0] or "Date" not in values[0]:
        print("[INFO] Sheet empty or malformed. Starting fresh.")
        last_date = "2015-01-01"
        existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame(values[1:], columns=values[0])
        existing_df.set_index("Date", inplace=True)
        existing_df.index = pd.to_datetime(existing_df.index)
        last_date = existing_df.index.max().strftime('%Y-%m-%d')
        print(f"[INFO] Last recorded date: {last_date}")
except Exception as e:
    print(f"[WARN] Could not read existing sheet: {e}")
    last_date = "2015-01-01"
    existing_df = pd.DataFrame()

# === Download only new historical data ===
today = datetime.today().strftime('%Y-%m-%d')
print(f"[INFO] Downloading data from {last_date} to {today}...")

df_new = yf.download(
    valid_tickers,
    start=last_date,
    end=today,
    interval="1d",
    auto_adjust=True
)['Close']

if df_new.empty:
    print("[INFO] No new data to update.")
    sys.exit(0)

df_new.index.name = "Date"
df_new = df_new[valid_tickers]
df_new.index = pd.to_datetime(df_new.index).normalize()

# === Merge with existing data ===
if not existing_df.empty:
    existing_df.index = pd.to_datetime(existing_df.index).normalize()
    df_combined = pd.concat([existing_df, df_new])
    df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
    print("[INFO] Merged with existing data.")
else:
    df_combined = df_new
    print("[INFO] No previous data found. Using fresh data.")

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

    # Clear only after confirming first chunk works
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

# === Upload to Google Sheets ===
df_to_write = df_combined.sort_index().reset_index()
try:
    sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
    update_sheet_in_chunks(sheet, df_to_write)
    print("✅ Upload to Google Sheets successful.")
except Exception as e:
    print("❌ Upload failed:", e)
    fallback_path = "Historical_Stocks.csv"
    df_to_write.to_csv(fallback_path, index=False)
    print(f"[📄] Fallback saved to {fallback_path}")
