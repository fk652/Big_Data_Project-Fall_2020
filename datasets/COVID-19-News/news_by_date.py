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

# Map Dates as Keys for the News
dates_keys = joined_news.map(lambda line: ((line.split(',')[0]).encode("utf-8"),(line.split(",")[1]).encode("utf-8")+"--"+(line.split(',')[2]).encode("utf-8")))

# Get news based on keyword COVID
filtered_covid_news = dates_keys.filter(lambda line: "COVID" in line[1])

# Get news based on keyword coronavirus
filtered_coronavirus_news = dates_keys.filter(lambda line: “coronavirus” in (line[1]).lower())

# Get news based on keyword stock
filtered_stock_news = dates_keys.filter(lambda line: "stock" in (line[1]).lower())

# Get news based on the keyword market
filtered_market_news = dates_keys.filter(lambda line: "market" in (line[1]).lower())

# Union between COVID and coronavirus news
first_union = filtered_covid_news.union(filtered_coronavirus_news)

# Union between stock and market news
second_union = filtered_stock_news.union(filtered_market_news)

# All news
all_news = first_union.union(second_union)

# Get all news by date comma separated
reduced_keys = all_news.reduceByKey(lambda x,y: x+','+y)

# Sort by key date
sorted_dates = reduced_keys.sortByKey()

# Rejoin array of tuples into comma separated key,value
all_news_by_date = sorted_dates.map(lambda x: x[0]+','+x[1])

# Save as Text File
all_news_by_date.saveAsTextFile("all_news_by_date.out")

# Stop SparkContext
sc.stop()