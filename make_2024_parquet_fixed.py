from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("make_2024_parquet_fixed").getOrCreate()

schema_2024 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
])

in_csv = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv"
out_parquet = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_fixed"

df = (spark.read.option("header", "true").schema(schema_2024).csv(in_csv)
      .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
      .withColumn("pickup_day", F.dayofmonth("pickup_ts"))
      .withColumn("pickup_hour", F.hour("pickup_ts"))
)

(df.write.mode("overwrite")
   .partitionBy("pickup_day", "pickup_hour")
   .parquet(out_parquet))

spark.stop()
