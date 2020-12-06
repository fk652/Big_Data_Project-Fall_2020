from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, rank, sum, max
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName('sector_specific_day_calculations').getOrCreate()

    # reading in the data
    data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1]) 
    data = data.select('Symbol',
                        'Name',
                        'Date',
                        'Log Return') \
                .filter(col('Log Return') != '')
    data = data.withColumn('Log Return', col('Log Return').cast('float').alias('Log Return'))
    
    # drops per day
    drop_days = ['2020-03-16', '2020-03-12', '2020-03-09', '2020-06-11', '2020-03-18']
    drop_data = data.filter(col('Date').isin(drop_days))
    drop_data = drop_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    drop_window = Window.partitionBy(drop_data['Date']).orderBy(drop_data['Log Return'].asc())
    drop_data = drop_data.select('*', rank().over(drop_window).alias('Rank')) \
                        .orderBy(col('Date'), col('Rank'))
    drop_data = drop_data.select('Date',
                                'Rank',
                                'Symbol',
                                'Name',
                                'Log Return')

    top_drop = drop_data.filter(col('Rank') <= 1)

    # gains per day
    gain_days = ['2020-03-24', '2020-03-13', '2020-04-06', '2020-03-26', '2020-03-17']
    gain_data = data.filter(col('Date').isin(gain_days))
    gain_data = gain_data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    gain_window = Window.partitionBy(gain_data['Date']).orderBy(gain_data['Log Return'].desc())
    gain_data = gain_data.select('*', rank().over(gain_window).alias('Rank')) \
                        .orderBy(col('Date'), col('Rank'))
    gain_data = gain_data.select('Date',
                                'Rank',
                                'Symbol',
                                'Name',
                                'Log Return')
    
    top_gain = gain_data.filter(col('Rank') <= 1)


    # writing out all the data
    header = [tuple(drop_data.columns)]
    header = spark.createDataFrame(header)
    drop_data = header.union(drop_data)
    drop_data.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('Sector_Specific_Day_Drops.out')

    header = [tuple(top_drop.columns)]
    header = spark.createDataFrame(header)
    top_drop = header.union(top_drop)
    top_drop.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('Sector_Specific_Day_Drops_Top_1.out')

    header = [tuple(gain_data.columns)]
    header = spark.createDataFrame(header)
    gain_data = header.union(gain_data)
    gain_data.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('Sector_Specific_Day_Gains.out')

    header = [tuple(top_gain.columns)]
    header = spark.createDataFrame(header)
    top_gain = header.union(top_gain)
    top_gain.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('Sector_Specific_Day_Gains_Top_1.out')

    spark.stop()


