import yfinance as yf
import pandas as pd
from datetime import datetime
import os

from google.colab import drive
drive.mount('/content/drive')

# === Adjust these paths to match your actual file locations in Google Drive ===
TICKER_FILE = '/content/drive/MyDrive/Stocks/Tickers_Codes.csv'
HISTORICAL_FILE = '/content/drive/MyDrive/Stocks/Historical_Stocks.csv'

# === Read tickers from CSV file ===
tickers_df = pd.read_csv(TICKER_FILE)
tickers = tickers_df["ticker"].dropna().tolist()

# === Get today's date ===
today = datetime.today().strftime('%Y-%m-%d')

# === Download historical stock data ===
df = yf.download(tickers, start="2015-01-02", end=today, interval="1d", auto_adjust=True)['Close']

# === Check if data is available ===
if df.empty:
    print("No data available for today.")
else:
    # Ensure columns are ordered as in the original tickers list
    df = df[tickers]

    # === If file exists, append new data ===
    if os.path.exists(HISTORICAL_FILE):
        df_existing = pd.read_csv(HISTORICAL_FILE, index_col=0)

        # Check if today's data is already in the file
        if today not in df_existing.index:
            df_combined = pd.concat([df_existing, df])
            df_combined.to_csv(HISTORICAL_FILE)
            print("Today's data added to the file.")
        else:
            print("Data for today already exists. No duplication.")
    else:
        # If file doesn't exist, create it
        df.to_csv(HISTORICAL_FILE)
        print("File created and today's data saved.")