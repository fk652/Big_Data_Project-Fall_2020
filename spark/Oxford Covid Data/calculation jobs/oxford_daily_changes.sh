#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

cd ../../../datasets/'Oxford Covid Data'/oxford_spark_calculations

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r oxford_daily_changes.out
rm -r oxford_daily_changes.csv

# uploading latest oxford data to hadoop
echo "uploading oxford covid data to hadoop"
hfs -rm -r oxford_clean.csv
hfs -put ../oxford_clean.csv

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python ../../../spark/'Oxford Covid Data'/'calculation jobs'/oxford_daily_changes.py oxford_clean.csv

# retrieve output from Hadoop
echo "retrieving oxford clean output"
hfs -getmerge oxford_daily_changes.out oxford_daily_changes.csv

echo "done"
