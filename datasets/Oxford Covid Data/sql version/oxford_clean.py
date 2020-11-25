from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date

if __name__ == "__main__":

    spark = SparkSession.builder.appName("oxford_clean").getOrCreate()

    data = spark.read.format('csv') \
                    .options(header='true', inferschema='false') \
                    .load(sys.argv[1]) 

    data = data.filter(data.CountryName == 'United States')

    data = data.withColumn('Date', to_date(unix_timestamp(col('Date'), 'yyyyMMdd').cast("timestamp")))

    int_columns = ['C1_School closing','C1_Flag','C2_Workplace closing','C2_Flag','C3_Cancel public events','C3_Flag','C4_Restrictions on gatherings','C4_Flag','C5_Close public transport','C5_Flag','C6_Stay at home requirements','C6_Flag','C7_Restrictions on internal movement','C7_Flag','C8_International travel controls','E1_Income support','E1_Flag','E2_Debt/contract relief','H1_Public information campaigns','H1_Flag','H2_Testing policy','H3_Contact tracing','H6_Facial Coverings','H6_Flag','ConfirmedCases','ConfirmedDeaths']
    for c in int_columns:
        data = data.withColumn(c, col(c).cast("int").alias(c))
    
    data = data.sort("RegionName", "Date")

    data.write.options(timestampFormat='yyyy-MM-dd', emptyValue='').csv('oxford_clean.out')

    spark.stop()
