#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc


parser = argparse.ArgumentParser()

parser.add_argument('--bucket', required=True)
parser.add_argument('--input_year', required=True)
parser.add_argument('--output_1', required=True)
parser.add_argument('--output_2', required=True)

args = parser.parse_args()

bucket= args.bucket
year = args.input_year
output_1 = args.output_1
output_2 = args.output_2




spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-1097968799226-kjm5jv3k')

for m in [1,2,3,4,5,6,7,8,9,10,11,12]:
            try:
                data = spark \
                .read \
                .format("parquet") \
                .option("header", "true") \
                .parquet(f"{bucket}/data/year={year}/month={m}/")



                data= data.select('rental_id',
                                'duration',
                                'bike_id',
                                date_trunc("day", "end_date").alias("end_date"),
                                'end_station_id',
                                'end_station_name',
                                date_trunc("day", "start_date").alias("start_date"),
                                'start_station_id',
                                'start_station_name')  



                data.registerTempTable('data')

                df_1 = spark.sql("""SELECT S.date,S.station_name,S.start_count,E.end_count
                                    FROM
                                        (SELECT  start_date as date, 
                                        count(bike_id) as start_count,
                                        start_station_name as station_name
                                        FROM data
                                        Group BY   date,start_station_name
                                        ORDER BY date ASC) AS S
                                    FULL OUTER JOIN
                                        (SELECT start_date as date, 
                                        count(bike_id) as end_count,
                                        end_station_name 
                                        FROM data
                                        Group BY   date,end_station_name
                                        ORDER BY date ASC) AS E
                                    ON S.date = E.date AND S.station_name = E.end_station_name;
                                    """)

                df_2 = spark.sql("""
                                    SELECT end_date as date, 
                                    count(bike_id) as count_roads,
                                    ROUND(AVG(duration/60),1) as avg_duration_minutes
                                    FROM data
                                    Group BY  end_date 
                                    ORDER BY date ASC;
                                """)

                df_1.write.format('bigquery') \
                    .option('table', output_1) \
                    .mode('append') \
                    .save()
                
                df_2.write.format('bigquery') \
                    .option('table', output_2) \
                    .mode('append') \
                    .save()
                
            except:
                continue

            