#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

./oxford_clean.sh 

cd 'calculation jobs'
./oxford_daily_changes.sh
./oxford_calculations.sh