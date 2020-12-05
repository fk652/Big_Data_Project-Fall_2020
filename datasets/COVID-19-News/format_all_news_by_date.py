from __future__ import print_function

import sys
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date

if __name__ == "__main__":

    spark = SparkSession.builder.appName('format all news by date').getOrCreate()

    lines = spark.sparkContext.textFile(sys.argv[1], 1)

    def formatMap(line):
        line = next(reader([line]))

        if line[0] == 'date':
            return []
        else:
            key = line[0]
            value = "\t".join(line[1:])
            return [(key, value)]

    newsData = lines.flatMap(formatMap)
    newsData = newsData.toDF(['date', 'title--source'])
    newsData = newsData.withColumn('date', to_date(unix_timestamp(col('date'), 'yyyy-MM-dd').cast('timestamp')))
    newsData = newsData.orderBy(col('date'))

    header = [tuple(newsData.columns)]
    header = spark.createDataFrame(header)
    newsData = header.union(newsData)
    newsData.write.options(emptyValue='').csv('all_news_by_date.out')

    spark.stop()

# spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python format_all_news_by_date.py 'all_news_by_date.csv'
# hfs -getmerge 'all_news_by_date.out' 'all_news_by_date.csv'