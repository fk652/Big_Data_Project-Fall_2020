#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

cd ../../../datasets/'Stock Data'/spark_calculations/'S&P_500_aggregate'

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'S&P_500_aggregate_Log_Sums.out'
rm -r 'S&P_500_aggregate_Log_Sums.csv'

hfs -rm -r 'S&P_500_aggregate_Cum_Sums.out'
rm -r 'S&P_500_aggregate_Cum_Sums.csv'

hfs -rm -r 'S&P_500_aggregate_Top_5_Drops.out'
rm -r 'S&P_500_aggregate_Top_5_Drops.csv'

hfs -rm -r 'S&P_500_aggregate_Top_5_Gains.out'
rm -r 'S&P_500_aggregate_Top_5_Gains.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'S&P_500_aggregate_stock_data.csv'
hfs -put ../../'S&P_500_aggregate_stock_data.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ../../../../spark/'Stock Data'/'S&P_500_aggregate'/spark_calculations.py 'S&P_500_aggregate_stock_data.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'S&P_500_aggregate_Log_Sums.out' 'S&P_500_aggregate_Log_Sums.csv'
hfs -getmerge 'S&P_500_aggregate_Cum_Sums.out' 'S&P_500_aggregate_Cum_Sums.csv'
hfs -getmerge 'S&P_500_aggregate_Top_5_Drops.out' 'S&P_500_aggregate_Top_5_Drops.csv'
hfs -getmerge 'S&P_500_aggregate_Top_5_Gains.out' 'S&P_500_aggregate_Top_5_Gains.csv'

echo "done"