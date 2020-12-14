from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, rank, sum, max
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName('oxford_calculations').getOrCreate()

    # reading in the data
    schema = 'CountryName STRING, \
            CountryCode STRING, \
            Date STRING, \
            ConfirmedCases INT, \
            ConfirmedCases_PercentChange FLOAT, \
            ConfirmedCases_LogChange FLOAT, \
            ConfirmedDeaths INT, \
            ConfirmedDeaths_PercentChange FLOAT, \
            ConfirmedDeaths_LogChange FLOAT, \
            StringencyIndexForDisplay FLOAT, \
            StringencyIndexForDisplay_PercentChange FLOAT, \
            StringencyIndexForDisplay_LogChange FLOAT, \
            StringencyLegacyIndexForDisplay FLOAT, \
            StringencyLegacyIndexForDisplay_PercentChange FLOAT, \
            StringencyLegacyIndexForDisplay_LogChange FLOAT, \
            GovernmentResponseIndexForDisplay FLOAT, \
            GovernmentResponseIndexForDisplay_PercentChange FLOAT, \
            GovernmentResponseIndexForDisplay_LogChange FLOAT, \
            ContainmentHealthIndexForDisplay FLOAT, \
            ContainmentHealthIndexForDisplay_PercentChange FLOAT, \
            ContainmentHealthIndexForDisplay_LogChange FLOAT, \
            EconomicSupportIndexForDisplay FLOAT, \
            EconomicSupportIndexForDisplay_PercentChange FLOAT, \
            EconomicSupportIndexForDisplay_LogChange FLOAT'

    data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .schema(schema) \
                    .load(sys.argv[1]) 

    data = data.select('Date',
                        'StringencyIndexForDisplay',
                        'StringencyIndexForDisplay_LogChange',
                        'StringencyLegacyIndexForDisplay',
                        'StringencyLegacyIndexForDisplay_LogChange',
                        'GovernmentResponseIndexForDisplay',
                        'GovernmentResponseIndexForDisplay_LogChange',
                        'ContainmentHealthIndexForDisplay',
                        'ContainmentHealthIndexForDisplay_LogChange',
                        'EconomicSupportIndexForDisplay',
                        'EconomicSupportIndexForDisplay_LogChange')
    data = data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    data = data.toDF('Date',
                    'USA_StringencyIndex',
                    'USA_StringencyIndex_LogChange',
                    'USA_StringencyLegacyIndex',
                    'USA_StringencyLegacyIndex_LogChange',
                    'USA_GovernmentResponseIndex',
                    'USA_GovernmentResponseIndex_LogChange',
                    'USA_ContainmentHealthIndex',
                    'USA_ContainmentHealthIndex_LogChange',
                    'USA_EconomicSupportIndex',
                    'USA_EconomicSupportIndex_LogChange')

    # cumulative log return
    cum_window = Window.orderBy(data['Date']).rangeBetween(Window.unboundedPreceding, 0)
    cum_sum = data.withColumn('USA_StringencyIndex_Cumulative', sum('USA_StringencyIndex_LogChange').over(cum_window))
    cum_sum = cum_sum.withColumn('USA_StringencyLegacyIndex_Cumulative', sum('USA_StringencyLegacyIndex_LogChange').over(cum_window))
    cum_sum = cum_sum.withColumn('USA_GovernmentResponseIndex_Cumulative', sum('USA_GovernmentResponseIndex_LogChange').over(cum_window))
    cum_sum = cum_sum.withColumn('USA_ContainmentHealthIndex_Cumulative', sum('USA_ContainmentHealthIndex_LogChange').over(cum_window))
    cum_sum = cum_sum.withColumn('USA_EconomicSupportIndex_Cumulative', sum('USA_EconomicSupportIndex_LogChange').over(cum_window))

    cum_sum = cum_sum.select('Date',
                            'USA_StringencyIndex',
                            'USA_StringencyIndex_LogChange',
                            'USA_StringencyIndex_Cumulative',
                            'USA_StringencyLegacyIndex',
                            'USA_StringencyLegacyIndex_LogChange',
                            'USA_StringencyLegacyIndex_Cumulative',
                            'USA_GovernmentResponseIndex',
                            'USA_GovernmentResponseIndex_LogChange',
                            'USA_GovernmentResponseIndex_Cumulative',
                            'USA_ContainmentHealthIndex',
                            'USA_ContainmentHealthIndex_LogChange',
                            'USA_ContainmentHealthIndex_Cumulative',
                            'USA_EconomicSupportIndex',
                            'USA_EconomicSupportIndex_LogChange',
                            'USA_EconomicSupportIndex_Cumulative')
    
    # total log return sum
    max_date = cum_sum.agg(max('Date')).collect()[0][0]
    sp_sum = cum_sum.where(cum_sum.Date == max_date) \
                    .drop('Date',
                        'USA_StringencyIndex',
                        'USA_StringencyIndex_LogChange',
                        'USA_StringencyLegacyIndex',
                        'USA_StringencyLegacyIndex_LogChange',
                        'USA_GovernmentResponseIndex',
                        'USA_GovernmentResponseIndex_LogChange',
                        'USA_ContainmentHealthIndex',
                        'USA_ContainmentHealthIndex_LogChange',
                        'USA_EconomicSupportIndex',
                        'USA_EconomicSupportIndex_LogChange')
                        
    sp_sum = sp_sum.toDF('USA_StringencyIndex_Sum',
                        'USA_StringencyLegacyIndex_Sum',
                        'USA_GovernmentResponseIndex_Sum',
                        'USA_ContainmentHealthIndex_Sum',
                        'USA_EconomicSupportIndex_Sum')
                            

    # writing out all the data
    header = [tuple(cum_sum.columns)]
    header = spark.createDataFrame(header)
    cum_sum = header.union(cum_sum)
    cum_sum.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('oxford_index_cumulative_sums.out')

    header = [tuple(sp_sum.columns)]
    header = spark.createDataFrame(header)
    sp_sum = header.union(sp_sum)
    sp_sum.write.options(emptyValue='').csv('oxford_index_total_sums.out')

    spark.stop()