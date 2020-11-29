#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'Sector_Specific_Day_Drops.out'
rm -r 'Sector_Specific_Day_Drops.out'

hfs -rm -r 'Sector_Specific_Day_Drops_Top_1.out'
rm -r 'Sector_Specific_Day_Drops_Top_1.out'

hfs -rm -r 'Sector_Specific_Day_Gains.out'
rm -r 'Sector_Specific_Day_Gains.out'

hfs -rm -r 'Sector_Specific_Day_Gains_Top_1.out'
rm -r 'Sector_Specific_Day_Gains_Top_1.out'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'Sectoral_stock_data.csv'
hfs -put ../../'Sectoral_stock_data.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python spark_day_drops_gains.py 'Sectoral_stock_data.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'Sector_Specific_Day_Drops.out' 'Sector_Specific_Day_Drops.csv'
hfs -getmerge 'Sector_Specific_Day_Drops_Top_1.out' 'Sector_Specific_Day_Drops_Top_1.csv'
hfs -getmerge 'Sector_Specific_Day_Gains.out' 'Sector_Specific_Day_Gains.csv'
hfs -getmerge 'Sector_Specific_Day_Gains_Top_1.out' 'Sector_Specific_Day_Gains_Top_1.csv'

echo "done"

