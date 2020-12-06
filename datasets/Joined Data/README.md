# Joined Datasets
This data is the combination of all other datasets:
- stock data
- oxford data
- john-hopkins
- covid news

Joins are done using spark in NYU's Hadoop cluster

We use joined datasets to compare and visualize relations between stock data and various covid information (U.S government policy, case and death count, and news)
___
### Join Data Directory
- S&P_500_aggregate
  - S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.csv

    Joins S&P 500 aggregate stock data, oxford data, john-hopkins data, and covid news data

  calculation jobs joined with news
    - top 5 drops
    - top 5 gains
- S&P_500 folder
  - S&P_500_Join_Oxford_JohnHopkins_News.zip
    
    **Note that this csv file is stored in a zip because of how large it is (~ 270 MB)**

    Joins S&P 500 stock data, oxford data, john-hopkins data, and covid news data

  calculation jobs joined with news
    - specific day drops
    - specific day top drops
    - specific day gains
    - specific day top gains
    - top 5 drops
    - top 5 gains
- Sectoral folder
  - Sector_Join_Oxford_JohnHopkins_News.csv

    Joins sector stock data, oxford data, john-hopkins data, and covid news data

  calculation jobs joined with news
    - specific day drops
    - specific day top drops
    - specific day gains
    - specific day top gains
    - top 1 drops
    - top 1 gains

