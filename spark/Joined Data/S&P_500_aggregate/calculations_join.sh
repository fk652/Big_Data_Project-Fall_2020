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
hfs -rm -r 'S&P_500_aggregate_Top_5_Gains_Join_News.out' 
rm -r 'S&P_500_aggregate_Top_5_Gains_Join_News.csv'

hfs -rm -r 'S&P_500_aggregate_Top_5_Drops_Join_News.out' 
rm -r 'S&P_500_aggregate_Top_5_Drops_Join_News.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'S&P_500_aggregate_Top_5_Gains.csv' 
hfs -put ../../'Stock%20Data'/'spark_calculations'/'S&P_500_aggregate'/'S&P_500_aggregate_Top_5_Gains.csv' 

hfs -rm -r 'S&P_500_aggregate_Top_5_Drops.csv' 
hfs -put ../../'Stock%20Data'/'spark_calculations'/'S&P_500_aggregate'/'S&P_500_aggregate_Top_5_Drops.csv' 

hfs -rm -r 'all_news_by_date.csv'
hfs -put ../../'COVID-19-News'/'all_news_by_date.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ../../../spark/'Joined Data'/'S&P_500_aggregate'/calculations_join.py 'S&P_500_aggregate_Top_5_Gains.csv' 'S&P_500_aggregate_Top_5_Drops.csv' 'all_news_by_date.csv' 

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'S&P_500_aggregate_Top_5_Gains_Join_News.out' 'S&P_500_aggregate_Top_5_Gains_Join_News.csv'
hfs -getmerge 'S&P_500_aggregate_Top_5_Drops_Join_News.out' 'S&P_500_aggregate_Top_5_Drops_Join_News.csv'

echo "done"