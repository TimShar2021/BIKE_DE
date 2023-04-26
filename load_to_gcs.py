import os
from prefect import flow, task
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
from pyspark.sql.functions import col
from variables import credentials_location, my_bucket, home



conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('test') \
        .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

@task()
def write_gcs(year) -> None:
    """Upload local file to GCS"""

    schema  = types.StructType([
                types.StructField("Rental Id", types.IntegerType(), True), 
                types.StructField("Duration", types.IntegerType(), True), 
                types.StructField("Bike Id", types.IntegerType(), True), 
                types.StructField("End Date", types.TimestampType(), True), 
                types.StructField("EndStation Id", types.IntegerType(), True), 
                types.StructField("EndStation Name", types.StringType(), True), 
                types.StructField("Start Date", types.TimestampType(), True), 
                types.StructField("StartStation Id", types.IntegerType(), True), 
                types.StructField("StartStation Name", types.StringType(), True),
                types.StructField("month", types.IntegerType(), True),
                types.StructField("year", types.IntegerType(), True)
            ])
    for address, dirs, files in os.walk(f"{home}data/{year}/"):
        for name in files:
            df = spark \
                .read \
                .schema(schema)\
                .format("csv") \
                .option("compression", "gzip") \
                .option("header", "true") \
                .csv(os.path.join(address, name))
            

            df = df.select(col("Rental Id").alias("rental_id"),
                col("Duration").alias("duration"),
                col("Bike Id").alias("bike_id"),
                col("End Date").alias("end_date"),
                col("EndStation Id").alias("end_station_id"),
                col("EndStation Name").alias("end_station_name"),
                col("Start Date").alias("start_date"),
                col("StartStation Id").alias("start_station_id"),
                col("StartStation Name").alias("start_station_name"),
                'year','month')

            
            df\
                .write \
                .format("parquet")\
                .partitionBy("year", "month") \
                .mode("append") \
                .save(f"{my_bucket}/data/")

@flow()
def load_to_gcs( years: list = ['2016']):
    for year in years:
        write_gcs(year)


if __name__ == "__main__":
     years = ['2016']
     load_to_gcs(years)
