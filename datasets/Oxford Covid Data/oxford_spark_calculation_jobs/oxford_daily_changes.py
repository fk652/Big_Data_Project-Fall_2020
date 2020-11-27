from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, lag, when, isnull, log
from pyspark.sql.window import Window

if __name__ == "__main__":

    spark = SparkSession.builder.appName('oxford_daily_changes').getOrCreate()

    data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1]) 

    data = data.select('CountryName', 
                        'CountryCode',
                        'Date',
                        'ConfirmedCases', 
                        'ConfirmedDeaths', 
                        'StringencyIndexForDisplay', 
                        'StringencyLegacyIndexForDisplay',
                        'GovernmentResponseIndexForDisplay',
                        'ContainmentHealthIndexForDisplay', 
                        'EconomicSupportIndexForDisplay')

    data = data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyy-MM-dd').cast('timestamp')))

    # previous day value column
    date_window = Window.orderBy('Date')
    data = data.withColumn('cases_prev', lag(data.ConfirmedCases).over(date_window))
    data = data.withColumn('deaths_prev', lag(data.ConfirmedDeaths).over(date_window))
    data = data.withColumn('stringency_prev', lag(data.StringencyIndexForDisplay).over(date_window))
    data = data.withColumn('legacy_prev', lag(data.StringencyLegacyIndexForDisplay).over(date_window))
    data = data.withColumn('government_prev', lag(data.GovernmentResponseIndexForDisplay).over(date_window))
    data = data.withColumn('containment_prev', lag(data.ContainmentHealthIndexForDisplay).over(date_window))
    data = data.withColumn('economic_prev', lag(data.EconomicSupportIndexForDisplay).over(date_window))

    # percent changes column
    data = data.withColumn('ConfirmedCases_PercentChange', 
        when(isnull((data.cases_prev - data.ConfirmedCases)/data.ConfirmedCases), 0).otherwise((data.cases_prev - data.ConfirmedCases)/data.ConfirmedCases))
    data = data.withColumn('ConfirmedDeaths_PercentChange', 
        when(isnull((data.deaths_prev - data.ConfirmedDeaths)/data.ConfirmedDeaths), 0).otherwise((data.deaths_prev - data.ConfirmedDeaths)/data.ConfirmedDeaths))
    data = data.withColumn('StringencyIndexForDisplay_PercentChange', 
        when(isnull((data.stringency_prev - data.StringencyIndexForDisplay)/data.StringencyIndexForDisplay), 0).otherwise((data.stringency_prev - data.StringencyIndexForDisplay)/data.StringencyIndexForDisplay))
    data = data.withColumn('StringencyLegacyIndexForDisplay_PercentChange', 
        when(isnull((data.legacy_prev - data.StringencyLegacyIndexForDisplay)/data.StringencyLegacyIndexForDisplay), 0).otherwise((data.legacy_prev - data.StringencyLegacyIndexForDisplay)/data.StringencyLegacyIndexForDisplay))
    data = data.withColumn('GovernmentResponseIndexForDisplay_PercentChange', 
        when(isnull((data.government_prev - data.GovernmentResponseIndexForDisplay)/data.GovernmentResponseIndexForDisplay), 0).otherwise((data.government_prev - data.GovernmentResponseIndexForDisplay)/data.GovernmentResponseIndexForDisplay))
    data = data.withColumn('ContainmentHealthIndexForDisplay_PercentChange', 
        when(isnull((data.containment_prev - data.ContainmentHealthIndexForDisplay)/data.ContainmentHealthIndexForDisplay), 0).otherwise((data.containment_prev - data.ContainmentHealthIndexForDisplay)/data.ContainmentHealthIndexForDisplay))
    data = data.withColumn('EconomicSupportIndexForDisplay_PercentChange', 
        when(isnull((data.economic_prev - data.EconomicSupportIndexForDisplay)/data.EconomicSupportIndexForDisplay), 0).otherwise((data.economic_prev - data.EconomicSupportIndexForDisplay)/data.EconomicSupportIndexForDisplay))

    # log percent changes column
    data = data.withColumn('ConfirmedCases_LogChange', 
        when(isnull(log(data.ConfirmedCases + 1) - log(data.cases_prev + 1)), 0).otherwise(log(data.ConfirmedCases + 1) - log(data.cases_prev + 1)))
    data = data.withColumn('ConfirmedDeaths_LogChange', 
        when(isnull(log(data.ConfirmedDeaths + 1) - log(data.deaths_prev + 1)), 0).otherwise(log(data.ConfirmedDeaths + 1) - log(data.deaths_prev + 1)))
    data = data.withColumn('StringencyIndexForDisplay_LogChange', 
        when(isnull(log(data.StringencyIndexForDisplay + 1) - log(data.stringency_prev + 1)), 0).otherwise(log(data.StringencyIndexForDisplay + 1) - log(data.stringency_prev + 1)))
    data = data.withColumn('StringencyLegacyIndexForDisplay_LogChange', 
        when(isnull(log(data.StringencyLegacyIndexForDisplay + 1) - log(data.legacy_prev + 1)), 0).otherwise(log(data.StringencyLegacyIndexForDisplay + 1) - log(data.legacy_prev + 1)))
    data = data.withColumn('GovernmentResponseIndexForDisplay_LogChange', 
        when(isnull(log(data.GovernmentResponseIndexForDisplay + 1) - log(data.government_prev + 1)), 0).otherwise(log(data.GovernmentResponseIndexForDisplay + 1) - log(data.government_prev + 1)))
    data = data.withColumn('ContainmentHealthIndexForDisplay_LogChange', 
        when(isnull(log(data.ContainmentHealthIndexForDisplay + 1) - log(data.containment_prev + 1)), 0).otherwise(log(data.ContainmentHealthIndexForDisplay + 1) - log(data.containment_prev + 1)))
    data = data.withColumn('EconomicSupportIndexForDisplay_LogChange', 
        when(isnull(log(data.EconomicSupportIndexForDisplay + 1) - log(data.economic_prev + 1)), 0).otherwise(log(data.EconomicSupportIndexForDisplay + 1) - log(data.economic_prev + 1)))

    # selecting final columns in order
    data = data.select('CountryName', 
                        'CountryCode',
                        'Date',
                        'ConfirmedCases',
                        'ConfirmedCases_PercentChange',
                        'ConfirmedCases_LogChange',
                        'ConfirmedDeaths',
                        'ConfirmedDeaths_PercentChange',
                        'ConfirmedDeaths_LogChange',
                        'StringencyIndexForDisplay', 
                        'StringencyIndexForDisplay_PercentChange',
                        'StringencyIndexForDisplay_LogChange',
                        'StringencyLegacyIndexForDisplay',
                        'StringencyLegacyIndexForDisplay_PercentChange',
                        'StringencyLegacyIndexForDisplay_LogChange',
                        'GovernmentResponseIndexForDisplay',
                        'GovernmentResponseIndexForDisplay_PercentChange',
                        'GovernmentResponseIndexForDisplay_LogChange',
                        'ContainmentHealthIndexForDisplay',
                        'ContainmentHealthIndexForDisplay_PercentChange',
                        'ContainmentHealthIndexForDisplay_LogChange',
                        'EconomicSupportIndexForDisplay',
                        'EconomicSupportIndexForDisplay_PercentChange',
                        'EconomicSupportIndexForDisplay_LogChange')

    # data = data.sort('Date')

    header = [tuple(data.columns)]
    header = spark.createDataFrame(header)
    data = header.union(data)

    data.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('oxford_daily_changes.out')
    spark.stop()

