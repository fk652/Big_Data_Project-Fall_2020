#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5
module load spark/2.4.0

# installing dependencies, assumes pandas and numpy aready installed
echo "installing necessary modules"
pip install --user statsmodels
pip install --user scipy

cd ../../../datasets/'Stock Data'/'machine learning'

# running the ols script
echo "running ols"
python ../../../spark/'Stock Data'/'machine learning'/'ols.py'

echo "done"