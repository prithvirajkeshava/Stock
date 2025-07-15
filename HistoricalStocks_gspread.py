import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from time import sleep

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
ticker_sheet = client.open_by_key(TICKERS_SHEET_ID).sheet1
tickers_df = pd.DataFrame(ticker_sheet.get_all_records())
tickers = tickers_df["ticker"].dropna().tolist()
tickers = [t.replace('.', '-') for t in tickers]  # Convert BRK.B → BRK-B

# === Filter out bad tickers ===
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
    print("No valid tickers found.")
    exit()

# === Get today's date ===
today = datetime.today().strftime('%Y-%m-%d')

# === Download full history for valid tickers ===
df = yf.download(valid_tickers, start="2015-01-02", end=today, interval="1d", auto_adjust=True)['Close']
if df.empty:
    print("No data downloaded.")
    exit()

df.index.name = "Date"
df = df[valid_tickers]
df_combined = None

# === Try to load existing sheet data ===
try:
    history_sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
    existing_data = history_sheet.get_all_values()

    if not existing_data or not existing_data[0] or "Date" not in existing_data[0]:
        print("[INFO] Sheet empty or malformed. Starting fresh.")
        df_combined = df
    else:
        existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        if "Date" not in existing_df.columns:
            raise ValueError("No 'Date' column in existing sheet.")

        existing_df.set_index("Date", inplace=True)
        existing_df.index = pd.to_datetime(existing_df.index)
        df.index = pd.to_datetime(df.index)

        if today not in existing_df.index.strftime('%Y-%m-%d'):
            df_combined = pd.concat([existing_df, df])
            print("[INFO] Appended new data.")
        else:
            df_combined = existing_df
            print("[INFO] Today's data already exists. No update.")
except Exception as e:
    print("[WARN] Sheet not found or error reading it:", e)
    df_combined = df


# === Function: Chunked Sheet Update ===
def update_sheet_in_chunks(sheet, df, chunk_size=500, max_retries=3):
    """Update large Google Sheets in chunks with retry logic."""
    sheet.clear()
    data = [df.columns.tolist()] + df.astype(str).values.tolist()

    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        start_row = i + 1
        cell_range = f"A{start_row}:"

        for attempt in range(1, max_retries + 1):
            try:
                print(f"[UPLOAD] Uploading rows {start_row} to {start_row + len(chunk) - 1} (Attempt {attempt})...")
                sheet.update(values=chunk, range_name=cell_range)
                sleep(1)
                break  # Success, move to next chunk
            except Exception as e:
                print(f" Attempt {attempt} failed: {e}")
                if attempt == max_retries:
                    raise
                sleep(2)  # Wait before retry



# === Upload to Google Sheets or Fallback to CSV ===
if df_combined is not None:
    df_to_write = df_combined.reset_index()
    df_to_write.columns = [str(c) for c in df_to_write.columns]  # Sanitize column names

    try:
        history_sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
        update_sheet_in_chunks(history_sheet, df_to_write, chunk_size=500)
        print(" Google Sheet updated successfully in chunks.")
    except Exception as e:
        print(" Failed to upload to Google Sheets:", e)
        df_to_write.to_csv("Historical_Stocks.csv", index=False)
        print(" Fallback saved to Historical_Stocks.csv")
