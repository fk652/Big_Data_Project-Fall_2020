import sys
from csv import reader
from pyspark import SparkContext

# Throw Error in case of invalid number of arguments
if len(sys.argv) != 2:
        print("Missing Files")
        exit(-1)

# Initializing SparkContext agent
sc = SparkContext()

# Read in the all_news.txt file
all_news = sc.textFile(sys.argv[1])

# Comma separate each line by date
all_news_keys = all_news.map(lambda line: line[:10]+','+(line[10:]).replace(',', '--')+',cnn.com')

# Save output
all_news_keys.saveAsTextFile("all_news.out")

# Stop SparkContext agent
sc.stop()