#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

cd ../../../datasets/'Stock Data'/spark_calculations/'Sectoral'

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'Sector_Log_Sums.out'
rm -r 'Sector_Log_Sums.csv'

hfs -rm -r 'Sector_Cum_Sums.out'
rm -r 'Sector_Cum_Sums.csv'

hfs -rm -r 'Sector_Top_1.out'
rm -r 'Sector_Top_1.csv'

hfs -rm -r 'Sector_Bottom_1.out'
rm -r 'Sector_Bottom_1.csv'

hfs -rm -r 'Sector_Top_5_Drops.out'
rm -r 'Sector_Top_5_Drops.csv'

hfs -rm -r 'Sector_Top_5_Gains.out'
rm -r 'Sector_Top_5_Gains.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'Sectoral_stock_data.csv'
hfs -put ../../'Sectoral_stock_data.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ../../../../spark/'Stock Data'/'Sectoral'/spark_calculations.py 'Sectoral_stock_data.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'Sector_Log_Sums.out' 'Sector_Log_Sums.csv'
hfs -getmerge 'Sector_Cum_Sums.out' 'Sector_Cum_Sums.csv'
hfs -getmerge 'Sector_Top_1.out' 'Sector_Top_1.csv'
hfs -getmerge 'Sector_Bottom_1.out' 'Sector_Bottom_1.csv'
hfs -getmerge 'Sector_Top_5_Drops.out' 'Sector_Top_5_Drops.csv'
hfs -getmerge 'Sector_Top_5_Gains.out' 'Sector_Top_5_Gains.csv'

echo "done"