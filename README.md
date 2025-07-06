# Stock
## Details of This Project

This repository automates US stock data for S&P 500 stocks.

### Contents

1. **Tickers_Codes.csv**  
   A file containing a list of ticker codes used for data extraction.

2. **Main_Stock.py**  
   A Python script that:
   - Reads the tickers from `Tickers_Codes.csv`.
   - Downloads the latest **Close Price** for each ticker.
   - Retrieves key financial indicators including:
     - Market Capitalisation (`marketCap`)
     - Price-to-Earnings Ratios (`trailingPE`, `forwardPE`)
     - Volatility (`beta`)
     - Earnings per Share (`trailingEps`)
     - Industry (`industry`)
     - Sector (sector)
     - Number of Full-Time Employees (`fullTimeEmployees`)
     - Country (`country`)
     - EBITDA (`ebitda`)
     - Total Debt (`totalDebt`)
     - Total Revenue (`totalRevenue`)
     - Gross Profits (`grossProfits`)
     - Free Cash Flow (`freeCashflow`)
     - Company Name (`shortName`)
     - PEG Ratio (`trailingPegRatio`) 
   - Saves this data into two separate files:
     - `Main_Actual_Stock.csv` → latest closing prices.
     - `Stock_Info.csv` → financial fundamentals.

3. **Tickers Info.xlsx**  
   An Excel file created from the official Wikipedia table of S&P 500 components, including:
   - Ticker symbol
   - Company name
   - Sector

4. **Historical_Stock.csv**  
   Contains the **last 10 years of daily Close Price data** for all tickers in `Tickers_Codes.csv`, updated via a similar Python script.

---

### Automation via GitHub Actions

This project utilises **GitHub Actions** for automatic every day updates at 9:30 PM (IST) or 4:00 PM (UTC).  
The scheduled workflow performs the following steps:

- Checks the repository
- Configures the Python environment
- Installs dependencies (`yfinance`, `pandas`)
- Executes the `Main_Stocks.py` script
- Commits and pushes the updated data to the repository
