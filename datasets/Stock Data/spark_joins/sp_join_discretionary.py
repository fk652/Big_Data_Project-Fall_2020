import sys
from csv import reader
from pyspark import SparkContext


sc = SparkContext()
stock = sc.textFile(sys.argv[1], 1)
info = sc.textFile(sys.argv[2], 1)

#disregard row with Symbol and create key-value pair for each dataset
lines_stock = stock.mapPartitions(lambda x: reader(x))\
.filter(lambda x: 'Symbol' not in x)\
.map(lambda x: ((x[1],(str('"'+x[2]+'"')) ),(x[0],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10]))) #map as (K,V) # Symbol key is Symbol[1] Name[2]

lines_info = info.mapPartitions(lambda x: reader(x))\
.filter(lambda line: 'Symbol' not in line)\
.filter(lambda x: 'Consumer Discretionary' in x)\
.map(lambda x: ((x[0],(str('"'+x[1]+'"'))),(x[2],(str('"'+x[3]+'"')),x[4],x[5],x[6],x[7]))) #map as (K,V) # key is 0

#join two datasets - (RDD[(K, V)],RDD[(K, W)]) to  RDD[(K, (V, W))] and sort rows by symbol, name, date
result = lines_stock.join(lines_info).sortBy(lambda x: (x[0],x[1][0][0]))\

#map result as K,V,W (removing spaces and parentheses)
output = result.map(lambda x: ','.join(x[0]) + ',' + ','.join(x[1][0])+ ',' + ','.join(x[1][1]))


#action
output.saveAsTextFile('spjoin_discretionary.out')

sc.stop()




