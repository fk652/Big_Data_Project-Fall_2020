import sys
from csv import reader
from pyspark import SparkContext

# Throw Error in case of invalid number of arguments
if len(sys.argv) != 2:
        print("Missing Files")
        exit(-1)

# Initializing SparkContext agent
sc = SparkContext()

# Read in the joined_news.csv File
joined_news = sc.textFile(sys.argv[1])

# Map date as key and news headline+source as value
dates_keys = lines.map(lambda line: ((line.split(',')[0]).encode("utf-8"),(line.split(',')[1]).encode("utf-8”)+'--'+(line.split(',')[2]).encode("utf-8”)))

# Reduce by key and combine news headlines+source by commas
reduced_keys = dates_keys.reduceByKey(lambda x,y: x+','+y)

# Sort tuples by key date
sorted_dates = reduced_keys.sortByKey()

# Comma separate key and value(s)
all_news_by_date = sorted_dates.map(lambda x: x[0]+’,’+x[1])

# Save as textFile
all_news_by_date.saveAsTextFile("sorted_by_date.out")

# Stop SparkContext agent
sc.stop()