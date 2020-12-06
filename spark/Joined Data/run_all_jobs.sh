#!/bin/bash

shopt -s expand_aliases
source /home/fk652/.bashrc

cd 'S&P_500'
./join.sh
./calculations_join.sh

cd ../'Sectoral'
./join.sh
./calculations_join.sh

cd ../'S&P_500_aggregate'
./join.sh
./calculations_join.sh