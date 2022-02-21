from typing import List
from pyspark.sql.functions import *
import pyspark.sql.functions as sfuncs
from pyspark.sql.types import ArrayType, StringType, MapType, IntegerType

import pyspark.sql as psql

from seldonite import collect

from geotext import GeoText
from geopy import geocoders

class Analyze():
    collector: collect.Collector

    def __init__(self, collector):
        self.collector = collector


    def process(self, spark_manager):
        df = self.collector.process(spark_manager)

        df = self.collector._fetch(spark_manager)
        if self.keywords_over_time_flag:
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))
            for keyword in keywords:
                df.withColumn(keyword, df['all_text'].like(f"%{keyword}%"))

    def keywords_over_time(self, keywords: List[str]):
        self.keywords_over_time_flag = True
        self.keywords = keywords

        return df


    def proportion_of_countries(self):
        self.collector._check_args()

        spark_builder = self.collector._get_spark_builder()

        with spark_builder.start_session() as spark_manager:
            df = self.collector._fetch(spark_manager)

            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

            def countries(x):
                country_list = GeoText(x).countries
                liste = []
                if not country_list:
                    return []
                else:
                    for country in country_list:
                        liste.append(country)
                    return liste

            udfCountry = udf(countries, ArrayType(StringType(), True))
            df = df.withColumn('countries', udfCountry(df.all_text))

            
            key = df.select(explode(df.countries).alias("key"))
            df_with_key = df.withColumn("key", lit(key))

            # TODO: what should I return?
            key.groupBy(col("key")).count()
    

    def keywords_over_time_delal(self, year):
        self.collector._check_args()

        spark_builder = self.collector._get_spark_builder()

        with spark_builder.start_session() as spark_manager:
            df = self.collector._fetch(spark_manager)

            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

            year_date = sfuncs.year(col("date")).alias("date_year")
            month_date = sfuncs.month(col("date")).alias("date_month")
            df = df.withColumn('date_year', lit(year_date))
            df = df.withColumn('date_month', lit(month_date))


            for keyword in keywords:
                df = df.withColumn(keyword, sfuncs.col('all_text').like(f"%{keyword}%"))

            df = df.groupby('date_year', 'date_month').count(*keywords)

            return df
            
