from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql.types import DecimalType

if __name__ == "__main__":

    spark = SparkSession.builder.appName('oxford_clean').getOrCreate()

    data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1]) 

    data = data.filter((data.CountryName == 'United States') & (data.Jurisdiction == 'NAT_TOTAL'))

    data = data.drop("RegionName", "RegionCode")

    data = data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyyMMdd').cast('timestamp')))

    float_columns = ['C1_School closing',
                    'C2_Workplace closing',
                    'C3_Cancel public events',
                    'C4_Restrictions on gatherings',
                    'C5_Close public transport',
                    'C6_Stay at home requirements',
                    'C7_Restrictions on internal movement',
                    'C8_International travel controls',
                    'E1_Income support',
                    'E2_Debt/contract relief',
                    'E3_Fiscal measures', #
                    'E4_International support', #
                    'H1_Public information campaigns',
                    'H2_Testing policy',
                    'H3_Contact tracing',
                    'H4_Emergency investment in healthcare', #
                    'H5_Investment in vaccines', #
                    'H6_Facial Coverings',
                    'StringencyIndex',
                    'StringencyIndexForDisplay',
                    'StringencyLegacyIndex',
                    'StringencyLegacyIndexForDisplay',
                    'GovernmentResponseIndex',
                    'GovernmentResponseIndexForDisplay',
                    'ContainmentHealthIndex',
                    'ContainmentHealthIndexForDisplay',
                    'EconomicSupportIndex',
                    'EconomicSupportIndexForDisplay']
    for c in float_columns:
        data = data.withColumn(c, col(c).cast(DecimalType(38,2)).alias(c))

    int_columns = ['C1_Flag',
                    'C2_Flag',
                    'C3_Flag',
                    'C4_Flag',
                    'C5_Flag',
                    'C6_Flag',
                    'C7_Flag',
                    'E1_Flag',
                    'H1_Flag',
                    'H6_Flag',
                    'ConfirmedCases',
                    'ConfirmedDeaths']
    for c in int_columns:
        data = data.withColumn(c, col(c).cast('int').alias(c))

    data = data.sort('Date')

    header = [tuple(data.columns)]
    header = spark.createDataFrame(header)
    data = header.union(data)

    data.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('oxford_clean.out')
    spark.stop()

