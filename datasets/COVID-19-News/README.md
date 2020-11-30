## COVID-19 News Datasets

-----------------------------------------------------------------------------------------------------------------------------

#### COVID-19-News Directory
```
COVID-19-News
├── README.md
├── COVID-19 News Data Collection and Cleaning.ipynb - Notebook I used to call the News APIs and Clean the data using Pandas
├── all_news.csv - all CNN news from 11/01/2019 - 11/29/2020 in .csv format (CLEANED)
├── all_news.txt - concatenation of all .txt files in /cnn_website_news (UNCLEANED)
├── all_news_by_date.csv - all CNN news + API news grouped by Key (date) -- using news_by_date.py with PySpark on Dumbo HDFS
├── covid_api_news.csv - output of COVID-19 News Data Collection and Cleaning for News APIs 
├── news_cleaner.py - PySpark MapReduce program to clean all_news.txt and output to all_news.csv
├── news_by_date.py - PySpark MapReduce program takes one agrument joined_news.csv and returns all_news_by_date.csv
└── cnn_website_news
    ├── april_2020.txt
    ├── august_2020.txt
    ├── december_2019.txt
    ├── february_2020.txt
    ├── january_2020.txt
    ├── july_2020.txt
    ├── march_2020.txt
    ├── may_2020.txt
    ├── november_2019.txt
    ├── november_2020.txt
    ├── october_2020.txt
    └── README.txt
    └── september_2020.txt
```

-----------------------------------------------------------------------------------------------------------------------------

The argument for all_news_by_date.py is the file 'joined_news.csv', which can be concatenated using `cat all_news.csv covid_api_news.csv >> joined_news.csv` which will generate the file joined_news.csv.