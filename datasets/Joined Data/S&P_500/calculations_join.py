# S&P_500_Top_5_Gains.csv
# Symbol,Name,Date,Log Return

# S&P_500_Top_5_Drops.csv
# Symbol,Name,Date,Log Return

# S&P_500_Specific_Day_Gains.csv
# Date,Rank,Symbol,Name,GICS Sector,GICS Sub-Industry,Log Return

# S&P_500_Specific_Day_Gains_Top_10.csv
# Date,Rank,Symbol,Name,GICS Sector,GICS Sub-Industry,Log Return

# S&P_500_Specific_Day_Drops.csv
# Date,Rank,Symbol,Name,GICS Sector,GICS Sub-Industry,Log Return

# S&P_500_Specific_Day_Drops_Top_10.csv
# Date,Rank,Symbol,Name,GICS Sector,GICS Sub-Industry,Log Return

# all_news_by_date.csv
# date,title--source

from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName('calculations join').getOrCreate()

    # reading in the data
    top_5_gains_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1])
    top_5_gains_data = top_5_gains_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    top_5_drops_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[2])
    top_5_drops_data = top_5_drops_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    specific_day_gains_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[3])
    specific_day_gains_data = specific_day_gains_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    specific_day_gains_top_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[4])
    specific_day_gains_top_data = specific_day_gains_top_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    specific_day_drops_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[5])
    specific_day_drops_data = specific_day_drops_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    specific_day_drops_top_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[6])
    specific_day_drops_top_data = specific_day_drops_top_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    news_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[7]) 
    news_data = news_data.withColumn('date', to_date(unix_timestamp(col('date'), 'yyyy-MM-dd').cast('timestamp')))
    news_data = news_data.toDF('Date',
                                'News title--source')


    # joining each of the data to news
    top_5_gains_data = top_5_gains_data.join(news_data, on=['Date'], how='left') \
                                                        .orderBy(col('Symbol'), col('Log Return').desc())

    top_5_drops_data = top_5_drops_data.join(news_data, on=['Date'], how='left') \
                                                        .orderBy(col('Symbol'), col('Log Return'))

    specific_day_gains_data = specific_day_gains_data.join(news_data, on=['Date'], how='left') \
                                                    .orderBy(col('Date'), col('Rank'))

    specific_day_gains_top_data = specific_day_gains_top_data.join(news_data, on=['Date'], how='left') \
                                                        .orderBy(col('Date'), col('Rank'))

    specific_day_drops_data = specific_day_drops_data.join(news_data, on=['Date'], how='left') \
                                                    .orderBy(col('Date'), col('Rank'))

    specific_day_drops_top_data = specific_day_drops_top_data.join(news_data, on=['Date'], how='left') \
                                                        .orderBy(col('Date'), col('Rank'))

    # writing out all the data
    header = [tuple(top_5_gains_data.columns)]
    header = spark.createDataFrame(header)
    top_5_gains_data = header.union(top_5_gains_data)
    top_5_gains_data.write.options(emptyValue='').csv('S&P_500_Top_5_Gains_Join_News.out')

    header = [tuple(top_5_drops_data.columns)]
    header = spark.createDataFrame(header)
    top_5_drops_data = header.union(top_5_drops_data)
    top_5_drops_data.write.options(emptyValue='').csv('S&P_500_Top_5_Drops_Join_News.out')

    header = [tuple(specific_day_gains_data.columns)]
    header = spark.createDataFrame(header)
    specific_day_gains_data = header.union(specific_day_gains_data)
    specific_day_gains_data.write.options(emptyValue='').csv('S&P_500_Specific_Day_Gains_Join_News.out')

    header = [tuple(specific_day_gains_top_data.columns)]
    header = spark.createDataFrame(header)
    specific_day_gains_top_data = header.union(specific_day_gains_top_data)
    specific_day_gains_top_data.write.options(emptyValue='').csv('S&P_500_Specific_Day_Gains_Top_10_Join_News.out')

    header = [tuple(specific_day_drops_data.columns)]
    header = spark.createDataFrame(header)
    specific_day_drops_data = header.union(specific_day_drops_data)
    specific_day_drops_data.write.options(emptyValue='').csv('S&P_500_Specific_Day_Drops_Join_News.out')

    header = [tuple(specific_day_drops_top_data.columns)]
    header = spark.createDataFrame(header)
    specific_day_drops_top_data = header.union(specific_day_drops_top_data)
    specific_day_drops_top_data.write.options(emptyValue='').csv('S&P_500_Specific_Day_Drops_Top_10_Join_News.out')

    spark.stop()