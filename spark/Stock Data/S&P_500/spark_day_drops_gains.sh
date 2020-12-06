#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

cd ../../../datasets/'Stock Data'/spark_calculations/'S&P_500'

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'S&P_500_Specific_Day_Drops.out'
rm -r 'S&P_500_Specific_Day_Drops.out'

hfs -rm -r 'S&P_500_Specific_Day_Drops_Top_10.out'
rm -r 'S&P_500_Specific_Day_Drops_Top_10.out'

hfs -rm -r 'S&P_500_Specific_Day_Gains.out'
rm -r 'S&P_500_Specific_Day_Gains.out'

hfs -rm -r 'S&P_500_Specific_Day_Gains_Top_10.out'
rm -r 'S&P_500_Specific_Day_Gains_Top_10.out'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'S&P_500_stock_data.csv'
hfs -put ../../'S&P_500_stock_data.csv'

hfs -rm -r 'S&P_500_Information.csv'
hfs -put ../../'S&P_500_Information.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ../../../../spark/'Stock Data'/'S&P_500'/spark_day_drops_gains.py 'S&P_500_stock_data.csv' 'S&P_500_Information.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'S&P_500_Specific_Day_Drops.out' 'S&P_500_Specific_Day_Drops.csv'
hfs -getmerge 'S&P_500_Specific_Day_Drops_Top_10.out' 'S&P_500_Specific_Day_Drops_Top_10.csv'
hfs -getmerge 'S&P_500_Specific_Day_Gains.out' 'S&P_500_Specific_Day_Gains.csv'
hfs -getmerge 'S&P_500_Specific_Day_Gains_Top_10.out' 'S&P_500_Specific_Day_Gains_Top_10.csv'

echo "done"