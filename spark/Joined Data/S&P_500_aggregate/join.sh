#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

cd ../../../datasets/'Joined Data'/'S&P_500_aggregate'

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.out'
rm -r 'S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'S&P_500_aggregate_stock_data.csv' 
hfs -put ../../'Stock%20Data'/'S&P_500_aggregate_stock_data.csv' 

hfs -rm -r 'S&P_500_aggregate_Cum_Sums.csv' 
hfs -put ../../'Stock%20Data'/'spark_calculations'/'S&P_500_aggregate'/'S&P_500_aggregate_Cum_Sums.csv' 

hfs -rm -r 'oxford_index_cumulative_sums.csv' 
hfs -put ../../'Oxford%20Covid%20Data'/'oxford_spark_calculations'/'oxford_index_cumulative_sums.csv'

hfs -rm -r 'us-covid19-aggregate.csv' 
hfs -put ../../'John-Hopkins%20Covid%20Data'/'us-covid19-aggregate.csv' 

hfs -rm -r 'worldwide-covid19-aggregate.csv' 
hfs -put ../../'John-Hopkins%20Covid%20Data'/'worldwide-covid19-aggregate.csv' 

hfs -rm -r 'all_news_by_date.csv'
hfs -put ../../'COVID-19-News'/'all_news_by_date.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ../../../spark/'Joined Data'/'S&P_500_aggregate'/join.py 'S&P_500_aggregate_stock_data.csv' 'S&P_500_aggregate_Cum_Sums.csv' 'oxford_index_cumulative_sums.csv' 'us-covid19-aggregate.csv' 'worldwide-covid19-aggregate.csv' 'all_news_by_date.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.out' 'S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.csv'

echo "done"
