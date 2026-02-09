from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, TimestampType
)
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("convert_2015_csv_to_parquet").getOrCreate()

CSV2015 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv"
OUT2015 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024"

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("RateCodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(CSV2015)
)

# Convert datetime strings to proper timestamps (optional but recommended)
df = (
    df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType()))
      .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType()))
)

df.write.mode("overwrite").parquet(OUT2015)

spark.stop()

