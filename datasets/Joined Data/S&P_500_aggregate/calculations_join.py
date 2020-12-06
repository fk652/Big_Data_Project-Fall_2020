from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName('sp500_agg_calculations_join').getOrCreate()

    # reading in the data
    top_5_gains_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1])
    top_5_gains_data = top_5_gains_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    top_5_drops_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[2])
    top_5_drops_data = top_5_drops_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    news_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[3]) 
    news_data = news_data.withColumn('date', to_date(unix_timestamp(col('date'), 'yyyy-MM-dd').cast('timestamp')))
    news_data = news_data.toDF('Date',
                                'News title--source')


    # joining each of the data to news
    top_5_gains_data = top_5_gains_data.join(news_data, on=['Date'], how='left') \
                                                        .orderBy(col('Symbol'), col('Log Return').desc())

    top_5_drops_data = top_5_drops_data.join(news_data, on=['Date'], how='left') \
                                                        .orderBy(col('Symbol'), col('Log Return'))

    # writing out all the data
    header = [tuple(top_5_gains_data.columns)]
    header = spark.createDataFrame(header)
    top_5_gains_data = header.union(top_5_gains_data)
    top_5_gains_data.write.options(emptyValue='').csv('S&P_500_aggregate_Top_5_Gains_Join_News.out')

    header = [tuple(top_5_drops_data.columns)]
    header = spark.createDataFrame(header)
    top_5_drops_data = header.union(top_5_drops_data)
    top_5_drops_data.write.options(emptyValue='').csv('S&P_500_aggregate_Top_5_Drops_Join_News.out')


    spark.stop()