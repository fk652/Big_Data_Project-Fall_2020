#!/bin/bash

cd ../../datasets/COVID-19-News

echo “loading modules”
module load python/gnu/3.6.5
module load spark/2.4.0

echo “running NewsAPI”
python3 news_api.py

echo “running covid-19-news API”
python3 covid_news_api.py

echo “combining news_api.csv and covid_19_news_api_articles.csv into covid_api_news.csv”
cat news_api.csv covid_19_news_api_articles.csv >> covid_api_news.csv

echo “removing existing output files”
hfs -rm -r all_news_by_date.out 
hfs -rm -r all_news.out 

cd ./cnn_website_news/

echo “combining CNN news”
cat *.txt >> ../all_news.txt

cd ..

echo “uploading all CNN nexts to cluster”
hfs -rm -r “all_news.txt”
hfs -put all_news.txt

echo “running spark job - news_cleaner.py”
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ./news_cleaner.py /user/{net_id}/all_news.txt

echo “getting data”
hfs -getmerge “all_news.out” “all_news.csv”

echo “concatenating CNN news and API news”
cat all_news.csv covid_api_news.csv >> joined_news.csv

echo “running spark job - news_by_date.py”
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ./news_by_date.py /user/{net_id}/joined_news.csv

echo “getting data”
hfs -getmerge “all_news_by_date.out” “all_news_by_date.csv”

echo “News complete!”