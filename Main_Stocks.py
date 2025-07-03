import pandas as pd               
import yfinance as yf             
from datetime import datetime   
import os                      

tickers_df = pd.read_csv("Tickers_Codes.csv")                          
tickers = tickers_df["ticker"].dropna().tolist()                 
tickers_dict = {str(ticker): True for ticker in tickers}       

 
# Gets a DataFrame with dates as index and tickers as columns
df = yf.download(tickers, period="5d", interval="1d", auto_adjust=True)['Close']

df = df.tail(1)

dow_stats = {}

# fetch financial information
for i in tickers_dict:
    ticker_yf = yf.Ticker(i)        
    temp = ticker_yf.info                      

    
    attributes_of_interest = [
        "marketCap", "trailingPE", "forwardPE", "beta", "trailingEps",
        "industry", "sector", "fullTimeEmployees", "country", "ebitda",
        "totalDebt", "totalRevenue", "grossProfits", "freeCashflow",
        "shortName", "trailingPegRatio"
    ]

    # Create a filtered dictionary with only desired attributes
    filtered_data = {attr: temp.get(attr) for attr in attributes_of_interest}

    # Convert to DataFrame with a single row
    ticker_info = pd.DataFrame([filtered_data])

    # Add to main dictionary with ticker as key
    dow_stats[i] = ticker_info

# Concatenate all individual ticker DataFrames into one
# Creates a hierarchical index: ticker > row
all_stats_info = pd.concat(dow_stats, keys=dow_stats.keys(), names=['ticker', 'Index'])

#   Save the overall financial information
file_info_name = "Stock_Info.csv"
all_stats_info.to_csv(file_info_name)

#  Save the closing prices to another file
file_name = "Main_Actual_Stock.csv"

if not os.path.exists(file_name) or os.stat(file_name).st_size == 0:
    # If the file doesn't exist or is empty, save the complete df
    df.to_csv(file_name, index=True)  # The date index is saved as "Date" column
    print("File created with initial data.")
else:
    # If the file already exists, check if the date has been saved
    df_existing = pd.read_csv(file_name, index_col="Date")
    df_existing.index = pd.to_datetime(df_existing.index)

    new_date = df.index[0]  # Date of the new data

    if new_date not in df_existing.index:
        # If the new date isn't in the existing file, append it
        df_final = pd.concat([df_existing, df])
        df_final.to_csv(file_name)
        print(" Data updated.")
    else:
        # If the date already exists, do nothing
        pass