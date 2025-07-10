import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
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
        print("Sheet empty or malformed. Starting fresh.")
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
            print("Appended new data.")
        else:
            df_combined = existing_df
            print("Today's data already exists. No update.")
except Exception as e:
    print("Sheet not found or error reading it:", e)
    df_combined = df

# === Upload to Google Sheets ===
if df_combined is not None:
    df_to_write = df_combined.reset_index()

    # 🔒 FULL SANITIZATION to avoid Timestamp/NaN issues
    columns = [str(c) for c in df_to_write.columns]
    rows = df_to_write.astype(str).values.tolist()


    try:
        history_sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
        history_sheet.clear()
        history_sheet.update([columns] + rows)
        print("✅ Sheet updated successfully.")
    except Exception as e:
        print("❌ Failed to upload to Google Sheets:", e)
