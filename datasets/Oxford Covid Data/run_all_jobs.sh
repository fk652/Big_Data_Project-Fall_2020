#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

./oxford_clean.sh 

cd oxford_spark_jobs
./oxford_daily_changes.sh