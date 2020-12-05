#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'Sector_Top_5_Gains_Join_News.out' 
rm -r 'Sector_Top_5_Gains_Join_News.csv'

hfs -rm -r 'Sector_Top_5_Drops_Join_News.out' 
rm -r 'Sector_Top_5_Drops_Join_News.csv'

hfs -rm -r 'Sector_Specific_Day_Gains_Join_News.out' 
rm -r 'Sector_Specific_Day_Gains_Join_News.csv'

hfs -rm -r 'Sector_Specific_Day_Gains_Top_1_Join_News.out' 
rm -r 'Sector_Specific_Day_Gains_Top_1_Join_News.csv'

hfs -rm -r 'Sector_Specific_Day_Drops_Join_News.out' 
rm -r 'Sector_Specific_Day_Drops_Join_News.csv'

hfs -rm -r 'Sector_Specific_Day_Drops_Top_1_Join_News.out' 
rm -r 'Sector_Specific_Day_Drops_Top_1_Join_News.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'Sector_Top_5_Gains.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'Sectoral'/'Sector_Top_5_Gains.csv' 

hfs -rm -r 'Sector_Top_5_Drops.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'Sectoral'/'Sector_Top_5_Drops.csv' 

hfs -rm -r 'Sector_Specific_Day_Gains.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'Sectoral'/'Sector_Specific_Day_Gains.csv' 

hfs -rm -r 'Sector_Specific_Day_Gains_Top_1.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'Sectoral'/'Sector_Specific_Day_Gains_Top_1.csv' 

hfs -rm -r 'Sector_Specific_Day_Drops.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'Sectoral'/'Sector_Specific_Day_Drops.csv' 

hfs -rm -r 'Sector_Specific_Day_Drops_Top_1.csv' 
hfs -put ../../'Yahoo%20Finance%20Stock%20Market%20Data'/'spark_calculations'/'Sectoral'/'Sector_Specific_Day_Drops_Top_1.csv' 

hfs -rm -r 'all_news_by_date.csv'
hfs -put ../../'COVID-19-News'/'all_news_by_date.csv'

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python calculations_join.py 'Sector_Top_5_Gains.csv' 'Sector_Top_5_Drops.csv' 'Sector_Specific_Day_Gains.csv' 'Sector_Specific_Day_Gains_Top_1.csv' 'Sector_Specific_Day_Drops.csv' 'Sector_Specific_Day_Drops_Top_1.csv' 'all_news_by_date.csv' 

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'Sector_Top_5_Gains_Join_News.out' 'Sector_Top_5_Gains_Join_News.csv'
hfs -getmerge 'Sector_Top_5_Drops_Join_News.out' 'Sector_Top_5_Drops_Join_News.csv'
hfs -getmerge 'Sector_Specific_Day_Gains_Join_News.out' 'Sector_Specific_Day_Gains_Join_News.csv'
hfs -getmerge 'Sector_Specific_Day_Gains_Top_1_Join_News.out' 'Sector_Specific_Day_Gains_Top_1_Join_News.csv'
hfs -getmerge 'Sector_Specific_Day_Drops_Join_News.out' 'Sector_Specific_Day_Drops_Join_News.csv'
hfs -getmerge 'Sector_Specific_Day_Drops_Top_1_Join_News.out' 'Sector_Specific_Day_Drops_Top_1_Join_News.csv'

echo "done"