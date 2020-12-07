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

- <https://en.wikipedia.org/wiki/List_of_S%26P_500_companies>

Historical data is pulled from Yahoo Finance using the yfinance API

- One year historical data from Nov. 15 2019 - Nov.15 2020

___

## Stock Data Directory

- S&P_scrape.py
  
  python script used to collect all the stock data
- S&P_500_Information.csv

  S&P 500 list info scraped from Wikipedia table
- S&P_500_aggregate_stock_data.csv

  S&P 500 aggregate stock data)
- S&P_500_stock_data.csv

  S&P 500 companies stock data
- Sectoral_stock_data.csv

  11 major sector stock data
- spark_calculations folder
  - S&P_500_aggregate folder
    - S&P_500_aggregate_Cum_Sums.csv

      cumulative log sums

    - S&P_500_aggregate_Log_Sums.csv

      total log sum

    - S&P_500_aggregate_Top_5_Drops.csv

      top 5 drops

    - S&P_500_aggregate_Top_5_Gains.csv

      top 5 gains

    **Aggregate top 5 gains and top 5 drop days are used for S&P 500 and Sector specific day calculations in order to determine which companies and sectors did good or bad those days**

  - S&P_500 folder
    - S&P_500_Cum_Sums.csv

      cumulative log sums

    - S&P_500_Log_Sums_Rank.csv

      total log sums per company, sorted by rank order

    - S&P_500_Log_Sums_Symbol.csv

      total log sums per company, sorted by ticker name

    - S&P_500_Top_10.csv

      top 10 companies based on total log return

    - S&P_500_Bottom_10.csv

      bottom 10 companies based on total log return

    - S&P_500_Top_5_Drops.csv

      top 5 drops per company

    - S&P_500_Top_5_Gains.csv

      top 5 gains per company

    - specific day calculations folder
      - S&P_500_Specific_Day_Drops.csv

        all companies log return per specific drop day

      - S&P_500_Specific_Day_Drops_Top_10.csv

        top 10 companies with largest log return drop per specific day

      - S&P_500_Specific_Day_Gains.csv

        all companies log return per specific gain day

      - S&P_500_Specific_Day_Gains_Top_10.csv

        top 10 companies with largest log return gain per specific day

  - Sectoral folder
    - Sector_Cum_Sums.csv

      cumulative log sums

    - Sector_Log_Sums.csv

      total log sums per sector, sorted by rank order

    - Sector_Top_1.csv

      top 1 sector based on total log return

    - Sector_Bottom_1.csv

      bottom 1 sector based on total log return

    - Sector_Top_5_Drops.csv

      top 5 drops per sector

    - Sector_Top_5_Gains.csv

      top 5 gains per sector

    - specific day calculations folder
      - Sector_Specific_Day_Drops.csv

        all sector log return per specific drop day

      - Sector_Specific_Day_Drops_Top_1.csv

        top 1 sector with largest log return drop per specific day

      - Sector_Specific_Day_Gains.csv

        all sector log return per specific gain day

      - Sector_Specific_Day_Gains_Top_1.csv

        top 1 sector with largest log return gain per specific day
