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

# === Your spreadsheet IDs ===
TICKERS_SHEET_ID = "1KCKBgqszZAoJk_RYndRvndP0CwE548LcLJOA6A_PCEQ"
HISTORICAL_SHEET_ID = "15AbIBbwNGl6qThiZzxQuY6Dgk5UloEv5pUfIKlvGNDU"

# === Load tickers from Google Sheets ===
ticker_sheet = client.open_by_key(TICKERS_SHEET_ID).sheet1
tickers_df = pd.DataFrame(ticker_sheet.get_all_records())
tickers = tickers_df["ticker"].dropna().tolist()
tickers = [t.replace('.', '-') for t in tickers]

# === Filter valid tickers ===
valid_tickers = []
for t in tickers:
    try:
        if not yf.Ticker(t).history(period="1d").empty:
            valid_tickers.append(t)
    except:
        print(f"[WARN] Skipping invalid ticker: {t}")

# === Download historical data ===
today = datetime.today().strftime('%Y-%m-%d')
df = yf.download(valid_tickers, start="2015-01-02", end=today, interval="1d", auto_adjust=True)['Close']
if df.empty:
    print("No data available.")
    exit()

df = df[valid_tickers]
df.index.name = "Date"
df_combined = None

# === Load or initialize sheet data ===
try:
    history_sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
    existing_data = history_sheet.get_all_values()

    if not existing_data or not existing_data[0] or "Date" not in existing_data[0]:
        print("Empty or malformed sheet. Initializing fresh.")
        df_combined = df
    else:
        existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        if "Date" not in existing_df.columns:
            raise ValueError("Missing 'Date' column.")
        existing_df.set_index("Date", inplace=True)
        existing_df.index = pd.to_datetime(existing_df.index)
        df.index = pd.to_datetime(df.index)

        if today not in existing_df.index.strftime('%Y-%m-%d'):
            df_combined = pd.concat([existing_df, df])
            print("Today's data added.")
        else:
            df_combined = existing_df
            print("Today's data already exists.")
except Exception as e:
    print("No existing sheet or error reading it:", e)
    df_combined = df

# === Prepare and upload ===
if df_combined is not None:
    df_to_write = df_combined.reset_index()

    # 💥 This part solves your error completely
    df_to_write.columns = df_to_write.columns.map(str)
    df_to_write = df_to_write.astype(str)

    try:
        history_sheet.clear()
        history_sheet.update(
            [df_to_write.columns.tolist()] + df_to_write.values.tolist()
        )
        print("✅ Sheet updated successfully.")
    except Exception as e:
        print("❌ Upload failed:", e)
