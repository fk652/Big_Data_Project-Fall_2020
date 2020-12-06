#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

cd ../../../datasets/'Stock Data'/spark_calculations/'S&P_500'

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'S&P_500_Log_Sums_Rank.out'
rm -r 'S&P_500_Log_Sums_Rank.csv'

hfs -rm -r 'S&P_500_Log_Sums_Symbol.out'
rm -r 'S&P_500_Log_Sums_Symbol.csv'

hfs -rm -r 'S&P_500_Cum_Sums.out'
rm -r 'S&P_500_Cum_Sums.csv'

hfs -rm -r 'S&P_500_Top_10.out'
rm -r 'S&P_500_Top_10.csv'

hfs -rm -r 'S&P_500_Bottom_10.out'
rm -r 'S&P_500_Bottom_10.csv'

hfs -rm -r 'S&P_500_Top_5_Drops.out'
rm -r 'S&P_500_Top_5_Drops.csv'

hfs -rm -r 'S&P_500_Top_5_Gains.out'
rm -r 'S&P_500_Top_5_Gains.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'S&P_500_stock_data.csv'
hfs -put ../../'S&P_500_stock_data.csv'

hfs -rm -r 'S&P_500_Information.csv'
hfs -put ../../'S&P_500_Information.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ../../../../spark/'Stock Data'/'S&P_500'/spark_calculations.py 'S&P_500_stock_data.csv' 'S&P_500_Information.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'S&P_500_Log_Sums_Rank.out' 'S&P_500_Log_Sums_Rank.csv'
hfs -getmerge 'S&P_500_Log_Sums_Symbol.out' 'S&P_500_Log_Sums_Symbol.csv'
hfs -getmerge 'S&P_500_Cum_Sums.out' 'S&P_500_Cum_Sums.csv'
hfs -getmerge 'S&P_500_Top_10.out' 'S&P_500_Top_10.csv'
hfs -getmerge 'S&P_500_Bottom_10.out' 'S&P_500_Bottom_10.csv'
hfs -getmerge 'S&P_500_Top_5_Drops.out' 'S&P_500_Top_5_Drops.csv'
hfs -getmerge 'S&P_500_Top_5_Gains.out' 'S&P_500_Top_5_Gains.csv'

echo "done"