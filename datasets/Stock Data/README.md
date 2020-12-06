# Stock Market Data
The following stock data is used for analysis:
- S&P 500 companies
- S&P 500 aggregate (^GSPC)
- 11 major sectors
  - Technology (XLK)
  - Consumer Discretionary (XLY)
  - Communication Services (XLC)
  - Health Care (XLV)
  - Materials (XLB)
  - Consumer Staples (XLP)
  - Utilities (XLU)
  - Industrials (XLI)
  - Real Estate (XLRE)
  - Financials (XLF)
  - Energy (XLE)

S&P 500 list pulled from Wikipedia (Nov. 15 2020)
- https://en.wikipedia.org/wiki/List_of_S%26P_500_companies

Historical data is pulled from Yahoo Finance using the yfinance API
- One year historical data from Nov. 15 2019 - Nov.15 2020
___
### Stock Data Directory
- S&P_scrape.py (python script used to collect all the stock data)
- S&P_500_Information.csv (S&P 500 list info scraped from Wikipedia table)
- S&P_500_aggregate_stock_data.csv
- S&P_500_stock_data.csv
- Sectoral_stock_data.csv
- spark_calculations folder
  - S&P_500_aggregate folder
    - cumulative log sums
    - total log sum
    - top 5 drops
    - top 5 gains
    
    **Aggregate top 5 gains and top 5 drop days are used for S&P 500 and Sector specific day calculations in order to determine which companies and sectors did good or bad those days**
  - S&P_500 folder
    - cumulative log sums
    - total log sums ranking (2 files)
      - sorted by rank order
      - sorted by ticker name
    - top 10 companies based on total log return
    - bottom 10 companies based on total log return
    
    specific day calculations
      - drop days log return for every company
      - top 10 drops per day
      - gain days log return for every company
      - top 10 gains per day
  - Sectoral folder
    - cumulative log sums
    - total log sums ranking (2 files)
      - sorted by rank order
      - sorted by ticker name
    - top 1 companies based on total log return
    - bottom 1 companies based on total log return
    
    specific day calculations
      - drop days log return
      - top 1 drops per day
      - gain days log return
      - top 1 gains per day
