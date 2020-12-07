# Joined Datasets

This data is the combination of all other datasets:

- stock data
- oxford data
- john-hopkins
- covid news

Joins are done using PySpark in NYU's Hadoop cluster

We use joined datasets to compare and visualize relations between stock data and various covid information (U.S government policy, case and death count, and news)
___

## Join Data Directory

- S&P_500_aggregate
  - S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.csv

    S&P 500 aggregate stock data joined with oxford data, john-hopkins data, and covid news data

  - S&P_500_aggregate_Top_5_Drops_Join_News.csv

    top 5 drops joined with news

  - S&P_500_aggregate_Top_5_Gains_Join_News.csv

    top 5 gains joined with news

- S&P_500 folder
  - S&P_500_Join_Oxford_JohnHopkins_News.zip

    Joins S&P 500 stock data, oxford data, john-hopkins data, and covid news data

    **Note that this csv file is stored in a zip because of how large it is (~ 270 MB)**

  - S&P_500_Specific_Day_Drops_Join_News.csv
  
    all company log return per specific drop day joined with news

  - S&P_500_Specific_Day_Drops_Top_10_Join_News.csv

    top 10 drops per specific drop day joined with news

  - S&P_500_Specific_Day_Gains_Join_News.csv
  
    all company log return per specific gain day joined with news

  - S&P_500_Specific_Day_Gains_Top_10_Join_News.csv
  
    top 10 gains per specific gain day joined with news

  - S&P_500_Top_5_Drops_Join_News.csv
  
    top 5 drops per company joined with news

  - S&P_500_Top_5_Gains_Join_News.csv

    top 5 gains per company joined with news

- Sectoral folder
  - Sector_Join_Oxford_JohnHopkins_News.csv

    Joins sector stock data, oxford data, john-hopkins data, and covid news data

  - Sector_Specific_Day_Drops_Join_News.csv
  
    all sector log return per specific drop day joined with news

  - Sector_Specific_Day_Drops_Top_1_Join_News.csv

    top 1 sector drop per specific drop day joined with news

  - Sector_Specific_Day_Gains_Join_News.csv
  
    all sector log return per specific gain day joined with news

  - Sector_Specific_Day_Gains_Top_1_Join_News.csv
  
    top 1 sector gain per specific gain day joined with news

  - Sector_Top_5_Drops_Join_News.csv
  
    top 5 drops per sector joined with news

  - Sector_Top_5_Gains_Join_News.csv

    top 5 gains per sector joined with news
