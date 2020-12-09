#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

./'S&P_scrape.sh'

cd 'S&P_500'
./spark_calculations.sh
./spark_day_drops_gains.sh

cd ../'Sectoral'
./spark_calculations.sh
./spark_day_drops_gains.sh

cd ../'S&P_500_aggregate'
./spark_calculations.sh

cd ../'machine learning'
./ols.sh