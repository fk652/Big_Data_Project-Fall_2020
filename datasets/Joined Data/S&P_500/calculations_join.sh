#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'S&P_500_Top_5_Gains_Join_News.out' 
rm -r 'S&P_500_Top_5_Gains_Join_News.csv'

hfs -rm -r 'S&P_500_Top_5_Drops_Join_News.out' 
rm -r 'S&P_500_Top_5_Drops_Join_News.csv'

hfs -rm -r 'S&P_500_Specific_Day_Gains_Join_News.out' 
rm -r 'S&P_500_Specific_Day_Gains_Join_News.csv'

hfs -rm -r 'S&P_500_Specific_Day_Gains_Top_10_Join_News.out' 
rm -r 'S&P_500_Specific_Day_Gains_Top_10_Join_News.csv'

hfs -rm -r 'S&P_500_Specific_Day_Drops_Join_News.out' 
rm -r 'S&P_500_Specific_Day_Drops_Join_News.csv'

hfs -rm -r 'S&P_500_Specific_Day_Drops_Top_10_Join_News.out' 
rm -r 'S&P_500_Specific_Day_Drops_Top_10_Join_News.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'S&P_500_Top_5_Gains.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'S&P_500'/'S&P_500_Top_5_Gains.csv' 

hfs -rm -r 'S&P_500_Top_5_Drops.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'S&P_500'/'S&P_500_Top_5_Drops.csv' 

hfs -rm -r 'S&P_500_Specific_Day_Gains.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'S&P_500'/'S&P_500_Specific_Day_Gains.csv' 

hfs -rm -r 'S&P_500_Specific_Day_Gains_Top_10.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'S&P_500'/'S&P_500_Specific_Day_Gains_Top_10.csv' 

hfs -rm -r 'S&P_500_Specific_Day_Drops.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'S&P_500'/'S&P_500_Specific_Day_Drops.csv' 

hfs -rm -r 'S&P_500_Specific_Day_Drops_Top_10.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'S&P_500'/'S&P_500_Specific_Day_Drops_Top_10.csv' 

hfs -rm -r 'all_news_by_date.csv'
hfs -put ../../'COVID-19-News'/'all_news_by_date.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python calculations_join.py 'S&P_500_Top_5_Gains.csv' 'S&P_500_Top_5_Drops.csv' 'S&P_500_Specific_Day_Gains.csv' 'S&P_500_Specific_Day_Gains_Top_10.csv' 'S&P_500_Specific_Day_Drops.csv' 'S&P_500_Specific_Day_Drops_Top_10.csv' 'all_news_by_date.csv' 

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'S&P_500_Top_5_Gains_Join_News.out' 'S&P_500_Top_5_Gains_Join_News.csv'
hfs -getmerge 'S&P_500_Top_5_Drops_Join_News.out' 'S&P_500_Top_5_Drops_Join_News.csv'
hfs -getmerge 'S&P_500_Specific_Day_Gains_Join_News.out' 'S&P_500_Specific_Day_Gains_Join_News.csv'
hfs -getmerge 'S&P_500_Specific_Day_Gains_Top_10_Join_News.out' 'S&P_500_Specific_Day_Gains_Top_10_Join_News.csv'
hfs -getmerge 'S&P_500_Specific_Day_Drops_Join_News.out' 'S&P_500_Specific_Day_Drops_Join_News.csv'
hfs -getmerge 'S&P_500_Specific_Day_Drops_Top_10_Join_News.out' 'S&P_500_Specific_Day_Drops_Top_10_Join_News.csv'

echo "done"