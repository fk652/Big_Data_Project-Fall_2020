#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'Sectoral_Join_Oxford_JohnHopkins_News.out'
rm -r 'Sectoral_Join_Oxford_JohnHopkins_News.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'Sectoral_stock_data.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'Sectoral_stock_data.csv' 

hfs -rm -r 'Sector_Cum_Sums.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'Sectoral'/'Sector_Cum_Sums.csv' 

hfs -rm -r 'oxford_index_cumulative_sums.csv' 
hfs -put ../../'Oxford%20Datasets'/'oxford_spark_jobs'/'oxford_index_cumulative_sums.csv'

hfs -rm -r 'us-covid19-aggregate.csv' 
hfs -put ../../'John-Hopkins%20COVID-19'/'us-covid19-aggregate.csv' 

hfs -rm -r 'worldwide-covid19-aggregate.csv' 
hfs -put ../../'John-Hopkins%20COVID-19'/'worldwide-covid19-aggregate.csv' 

hfs -rm -r 'all_news_by_date.csv'
hfs -put ../../'COVID-19-News'/'all_news_by_date.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python join.py 'Sectoral_stock_data.csv' 'Sector_Cum_Sums.csv' 'oxford_index_cumulative_sums.csv' 'us-covid19-aggregate.csv' 'worldwide-covid19-aggregate.csv' 'all_news_by_date.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'Sectoral_Join_Oxford_JohnHopkins_News.out' 'Sectoral_Join_Oxford_JohnHopkins_News.csv'

echo "done"
