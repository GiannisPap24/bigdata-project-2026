from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check_schema_2024_parquet").getOrCreate()

p = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024"
df = spark.read.parquet(p)

print("=== PRINT SCHEMA (2024 PARQUET) ===")
df.printSchema()

print("=== SAMPLE ROWS (selected columns) ===")
df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "tip_amount"
).show(5, truncate=False)

spark.stop()
