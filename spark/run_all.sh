#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

cd 'Stock Data'
./run_all_jobs.sh

cd ../'Oxford Covid Data'
./run_all_jobs.sh

cd ../'Joined Data'
./run_all_jobs.sh