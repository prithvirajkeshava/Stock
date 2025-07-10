import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Setup Google Sheets credentials ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# === Your spreadsheet IDs ===
TICKERS_SHEET_ID = "1KCKBgqszZAoJk_RYndRvndP0CwE548LcLJOA6A_PCEQ"
HISTORICAL_SHEET_ID = "15AbIBbwNGl6qThiZzxQuY6Dgk5UloEv5pUfIKlvGNDU"

# === Load tickers from Google Sheets ===
ticker_sheet = client.open_by_key(TICKERS_SHEET_ID).sheet1
ticker_records = ticker_sheet.get_all_records()
tickers_df = pd.DataFrame(ticker_records)
tickers = tickers_df["ticker"].dropna().tolist()
tickers = [ticker.replace('.', '-') for ticker in tickers]  # Fix problematic tickers

# === Get today's date ===
today = datetime.today().strftime('%Y-%m-%d')

# === Download historical stock data ===
df = yf.download(tickers, start="2015-01-02", end=today, interval="1d", auto_adjust=True)['Close']

if df.empty:
    print("No data available for today.")
else:
    df = df[tickers]
    df.index.name = "Date"
    df_combined = None

    try:
        # === Try to load existing historical data ===
        history_sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
        existing_data = history_sheet.get_all_values()

        if not existing_data or not existing_data[0] or "Date" not in existing_data[0]:
            print("Sheet exists but empty or malformed. Writing fresh.")
            df_combined = df
        else:
            existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
            if "Date" not in existing_df.columns:
                raise ValueError("No 'Date' column found in existing sheet.")

            existing_df.set_index("Date", inplace=True)
            existing_df.index = pd.to_datetime(existing_df.index)
            df.index = pd.to_datetime(df.index)

            if today not in existing_df.index.strftime('%Y-%m-%d'):
                df_combined = pd.concat([existing_df, df])
                print("Today's data added.")
            else:
                df_combined = existing_df
                print("Data for today already exists. No update.")

    except Exception as e:
        print("No existing historical sheet or error reading it:", e)
        df_combined = df

    # === Save back to Google Sheets ===
    if df_combined is not None:
        # Create a copy for writing
        df_to_write = df_combined.copy()

        # Reset index and convert all values to strings
        df_to_write.reset_index(inplace=True)
        df_to_write = df_to_write.astype(str)

        # Upload to Google Sheets
        history_sheet = client.open_by_key(HISTORICAL_SHEET_ID).sheet1
        history_sheet.clear()
        history_sheet.update([df_to_write.columns.values.tolist()] + df_to_write.values.tolist())

        print("Updated Historical_Stocks sheet.")
