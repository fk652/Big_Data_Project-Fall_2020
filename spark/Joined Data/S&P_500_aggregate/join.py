from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, rank, sum, max, min
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName('sp500_agg_join').getOrCreate()

    # reading in the data
    stock_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1])
    stock_data = stock_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))
    stock_data = stock_data.toDF('Date',
                                'Symbol',
                                'Name',
                                'Close_Change',
                                'Close_LogChange',
                                'Close',
                                'Adj Close',
                                'High',
                                'Low',
                                'Open',
                                'Volume')

    max_date = stock_data.agg(max('Date')).collect()[0][0]
    min_date = stock_data.agg(min('Date')).collect()[0][0]

    stock_cumulative_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[2])
    stock_cumulative_data = stock_cumulative_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))
    stock_cumulative_data = stock_cumulative_data.select('Symbol',
                                                        'Date',
                                                        'CumulativeSum')
    stock_cumulative_data = stock_cumulative_data.toDF('Symbol',
                                                        'Date',
                                                        'Close_LogChangeCumulative')


    oxford_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[3]) 
    oxford_data = oxford_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))
    oxford_data = oxford_data.filter((col('Date') <= max_date) & (col('Date') >= min_date))
    oxford_data = oxford_data.toDF('Date',
                                'USA_StringencyIndex',
                                'USA_StringencyIndex_LogChange',
                                'USA_StringencyIndex_LogChangeCumulative',
                                'USA_StringencyLegacyIndex',
                                'USA_StringencyLegacyIndex_LogChange',
                                'USA_StringencyLegacyIndex_LogChangeCumulative',
                                'USA_GovernmentResponseIndex',
                                'USA_GovernmentResponseIndex_LogChange',
                                'USA_GovernmentResponseIndex_LogChangeCumulative',
                                'USA_ContainmentHealthIndex',
                                'USA_ContainmentHealthIndex_LogChange',
                                'USA_ContainmentHealthIndex_LogChangeCumulative',
                                'USA_EconomicSupportIndex',
                                'USA_EconomicSupportIndex_LogChange',
                                'USA_EconomicSupportIndex_LogChangeCumulative')

    john_hopkins_USA_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[4]) 
    john_hopkins_USA_data = john_hopkins_USA_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))
    john_hopkins_USA_data = john_hopkins_USA_data.filter((col('Date') <= max_date) & (col('Date') >= min_date))
    john_hopkins_USA_data = john_hopkins_USA_data.select('Date',
                                                        'Confirmed Cases',
                                                        'Cases Increase',
                                                        'Deaths',
                                                        'Deaths Increase')
    john_hopkins_USA_data = john_hopkins_USA_data.toDF('Date',
                                                        'USA Covid Confirmed Cases',
                                                        'USA Covid Cases LogChange',
                                                        'USA Covid Deaths',
                                                        'USA Covid Deaths LogChange')

    john_hopkins_world_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[5]) 
    john_hopkins_world_data = john_hopkins_world_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))
    john_hopkins_world_data = john_hopkins_world_data.filter((col('Date') <= max_date) & (col('Date') >= min_date))
    john_hopkins_world_data = john_hopkins_world_data.select('Date',
                                                            'Confirmed Cases',
                                                            'Cases Increase',
                                                            'Deaths',
                                                            'Deaths Increase')
    john_hopkins_world_data = john_hopkins_world_data.toDF('Date',
                                                        'World Covid Confirmed Cases',
                                                        'World Covid Cases LogChange',
                                                        'World Covid Deaths',
                                                        'World Covid Deaths LogChange')

    news_data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[6]) 
    news_data = news_data.withColumn('date', to_date(unix_timestamp(col('date'), 'yyyy-MM-dd').cast('timestamp')))
    news_data = news_data.filter((col('date') <= max_date) & (col('date') >= min_date))
    news_data = news_data.toDF('Date',
                                'News title--source')


    # John Hopkins cumulative increase calculations
    cum_window = Window.orderBy(john_hopkins_USA_data['Date']).rangeBetween(Window.unboundedPreceding, 0)
    john_hopkins_USA_data = john_hopkins_USA_data.withColumn('USA Covid Cases LogChangeCumulative', sum('USA Covid Cases LogChange').over(cum_window))
    john_hopkins_USA_data = john_hopkins_USA_data.withColumn('USA Covid Deaths LogChangeCumulative', sum('USA Covid Deaths LogChange').over(cum_window))
    john_hopkins_USA_data = john_hopkins_USA_data.select('Date',
                                                        'USA Covid Confirmed Cases',
                                                        'USA Covid Cases LogChange',
                                                        'USA Covid Cases LogChangeCumulative',
                                                        'USA Covid Deaths',
                                                        'USA Covid Deaths LogChange',
                                                        'USA Covid Deaths LogChangeCumulative')

    cum_window = Window.orderBy(john_hopkins_world_data['Date']).rangeBetween(Window.unboundedPreceding, 0)
    john_hopkins_world_data = john_hopkins_world_data.withColumn('World Covid Cases LogChangeCumulative', sum('World Covid Cases LogChange').over(cum_window))
    john_hopkins_world_data = john_hopkins_world_data.withColumn('World Covid Deaths LogChangeCumulative', sum('World Covid Deaths LogChange').over(cum_window))
    john_hopkins_world_data = john_hopkins_world_data.select('Date',
                                                        'World Covid Confirmed Cases',
                                                        'World Covid Cases LogChange',
                                                        'World Covid Cases LogChangeCumulative',
                                                        'World Covid Deaths',
                                                        'World Covid Deaths LogChange',
                                                        'World Covid Deaths LogChangeCumulative')

    # joining all the data
    joined_data = stock_data.join(stock_cumulative_data, on=['Date', 'Symbol'], how='inner')
    joined_data = joined_data.select('Date',
                                    'Symbol',
                                    'Name',
                                    'Close_Change',
                                    'Close_LogChange',
                                    'Close_LogChangeCumulative',
                                    'Close',
                                    'Adj Close',
                                    'High',
                                    'Low',
                                    'Open',
                                    'Volume')
    
    joined_data = joined_data.join(john_hopkins_USA_data, on=['Date'], how='outer') # outer

    joined_data = joined_data.join(john_hopkins_world_data, on=['Date'], how='outer') # outer
    
    joined_data = joined_data.join(oxford_data, on=['Date'], how='outer') # outer

    joined_data = joined_data.join(news_data, on=['Date'], how='outer') # outer

    joined_data = joined_data.orderBy(col('Date'), col('Symbol'))              

    # writing out all the data
    header = [tuple(joined_data.columns)]
    header = spark.createDataFrame(header)
    joined_data = header.union(joined_data)
    joined_data.write.options(emptyValue='').csv('S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.out')

    spark.stop()