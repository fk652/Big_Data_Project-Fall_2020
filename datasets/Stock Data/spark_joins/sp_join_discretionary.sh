
. /etc/profile.d/modules.sh
# load up necessary modules
echo "loading modules"
module load python/gnu/3.6.5 #load python version
module load spark/2.4.0 #load spark version

# remove existing output files
echo "removing existing output files"
#hfs -rm -r 'spjoin_discretionary.out'
#rm -r 'spjoin_discretionary.csv'

# uploading data to hadoop
#echo "uploading data to hadoop"
#hfs -rm -r 'Sectoral_stock_data.csv'
#hfs -put ../../'Sectoral_stock_data.csv'

# run spark job
echo "running spark job"
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
sp_join_discretionary.py SnP_500_stock_data.csv \
SnP_500_Information.csv

# retrieve output from Hadoop
echo "retrieving hadoop output"
hfs -getmerge 'spjoin_discretionary.out' 'spjoin_discretionary.csv'

echo "done"
