import yfinance as yf               
import pandas as pd                
from datetime import datetime      
import os                          
 
 
FILE_NAME = 'Historical_Stocks.csv'   
 
   
tickers_df = pd.read_csv("Tickers_Codes.csv")   
tickers = tickers_df["ticker"].dropna().tolist()   

  
today = datetime.today().strftime('%Y-%m-%d')

 
df = yf.download(tickers, start="2015-01-02", end=today, interval="1d", auto_adjust=True)['Close']

#  Check if data was retrieved (may fail on weekends or holidays)
if df.empty:
    print(" No data available for today.")
else:
    # Reorder columns to match the order of the tickers
    df = df[tickers]  

    #  Check if the CSV file already exists
    if os.path.exists(FILE_NAME):
        # Read existing file
        df_existing = pd.read_csv(FILE_NAME, index_col=0)

        # If today's date is not yet saved, add it
        if today not in df_existing.index:
            df_final = pd.concat([df_existing, df])
            df_final.to_csv(FILE_NAME)
            print(" Today's data added to the file.")
        else:
            print("  Data for today already exists. No duplication.")
    else:
        # If the file does not exist, create it with today's data
        df.to_csv(FILE_NAME)
        print("  File created and today's data saved.")