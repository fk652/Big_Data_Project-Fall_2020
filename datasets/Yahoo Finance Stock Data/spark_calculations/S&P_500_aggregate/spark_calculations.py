from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, rank, sum, max
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName('oxford_daily_changes').getOrCreate()

    # reading in the data
    data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1]) 
    data = data.select('Symbol',
                        'Name',
                        'Date',
                        'Log Return') \
                .filter(col('Log Return') != '')
    data = data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))
    data = data.withColumn('Log Return', col('Log Return').cast('float').alias('Log Return'))


    # cumulative log return sum
    cum_window = Window.partitionBy(data['Symbol']).orderBy(data['Date']).rangeBetween(Window.unboundedPreceding, 0)
    cum_sum = data.withColumn('CumulativeSum', sum('Log Return').over(cum_window))
    cum_sum = cum_sum.orderBy(col('Symbol'), col('Date'))
    
    # total log return sum
    sum_window = Window.partitionBy('Symbol')
    sp_sum = cum_sum.withColumn('MaxDate', max('Date').over(sum_window)) \
                    .where(col('Date') == col('MaxDate')) \
                    .drop('MaxDate', 'Log Return', 'Date') \
                    .withColumnRenamed('CumulativeSum', 'ReturnSum')
    sp_sum = sp_sum.select('Symbol',
                            'Name',
                            'ReturnSum')

    # top 5 drops
    top_drop_window = Window.partitionBy(data['Symbol']).orderBy(data['Log Return'].asc())
    top_5_drops = data.select('*', rank().over(top_drop_window).alias('rank')) \
                        .filter(col('rank') <= 5) \
                        .drop('rank') \
                        .orderBy(col('Symbol'), col('Log Return').asc())

    # top 5 gains
    top_gain_window = Window.partitionBy(data['Symbol']).orderBy(data['Log Return'].desc())
    top_5_gains = data.select('*', rank().over(top_drop_window).alias('rank')) \
                        .filter(col('rank') <= 5) \
                        .drop('rank') \
                        .orderBy(col('Symbol'), col('Log Return').desc())


    # writing out all the data
    header = [tuple(sp_sum.columns)]
    header = spark.createDataFrame(header)
    sp_sum = header.union(sp_sum)
    sp_sum.write.options(emptyValue='').csv('S&P_500_aggregate_Log_Sums.out')

    header = [tuple(cum_sum.columns)]
    header = spark.createDataFrame(header)
    cum_sum = header.union(cum_sum)
    cum_sum.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('S&P_500_aggregate_Cum_Sums.out')

    header = [tuple(top_5_drops.columns)]
    header = spark.createDataFrame(header)
    top_5_drops = header.union(top_5_drops)
    top_5_drops.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('S&P_500_aggregate_Top_5_Drops.out')

    header = [tuple(top_5_gains.columns)]
    header = spark.createDataFrame(header)
    top_5_gains = header.union(top_5_gains)
    top_5_gains.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('S&P_500_aggregate_Top_5_Gains.out')

    spark.stop()


