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

dates_keys = joined_news.map(lambda line: ((line.split(',')[0]).encode("utf-8"),(line.split(",")[1]).encode("utf-8")+"--"+(line.split(',')[2]).encode("utf-8")))

filtered_covid_news = dates_keys.filter(lambda line: "COVID" in line[1])

filtered_coronavirus_news = dates_keys.filter(lambda line: “coronavirus” in (line[1]).lower())

filtered_stock_news = dates_keys.filter(lambda line: "stock" in (line[1]).lower())

filtered_market_news = dates_keys.filter(lambda line: "market" in (line[1]).lower())

first_union = filtered_covid_news.union(filtered_coronavirus_news)

second_union = filtered_stock_news.union(filtered_market_news)

all_news = first_union.union(second_union)

reduced_keys = all_news.reduceByKey(lambda x,y: x+','+y)

sorted_dates = reduced_keys.sortByKey()

all_news_by_date = sorted_dates.map(lambda x: x[0]+','+x[1])

all_news_by_date.saveAsTextFile("all_news_by_date.out")

sc.stop()
