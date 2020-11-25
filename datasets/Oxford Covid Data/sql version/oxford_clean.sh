#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# remove existing output files
echo "removing existing output files"
hfs -rm -r oxford_clean.out
rm -r oxford_clean.csv

# uploading latest oxford data to hadoop
echo "uploading oxford covid data to hadoop"
hfs -rm -r OxCGRT_latest.csv
rm -r OxCGRT_latest.csv
wget  https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv
hfs -put OxCGRT_latest.csv

# run spark job
echo "running spark job"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python oxford_clean.py OxCGRT_latest.csv

# retrieve output from Hadoop
echo "retrieving oxford clean output"
hfs -getmerge oxford_clean.out oxford_clean.csv

# adding header to top of file
echo "adding header to top of csv file"
head -1 OxCGRT_latest.csv | cat - oxford_clean.csv > oxford_clean.csv."$$" && mv oxford_clean.csv."$$" oxford_clean.csv

echo "done cleaning oxford data"
