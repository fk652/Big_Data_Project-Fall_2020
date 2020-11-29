#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r 'oxford_index_cumulative_sums.out'
rm -r 'oxford_index_cumulative_sums.csv'

hfs -rm -r 'oxford_index_total_sums.out'
rm -r 'oxford_index_total_sums.csv'

# uploading latest data to hadoop
echo "uploading data to hadoop"
hfs -rm -r 'oxford_daily_changes.csv'
hfs -put 'oxford_daily_changes.csv'


# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python oxford_calculations.py 'oxford_daily_changes.csv'

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'oxford_index_cumulative_sums.out' 'oxford_index_cumulative_sums.csv'
hfs -getmerge 'oxford_index_total_sums.out' 'oxford_index_total_sums.csv'

echo "done"
