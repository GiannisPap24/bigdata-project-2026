from pyspark.sql import SparkSession

ZONES = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/taxi_zone_lookup"

spark = SparkSession.builder.appName("CHECK-SCHEMA-ZONES").getOrCreate()
z = spark.read.parquet(ZONES)
print("COLUMNS:", z.columns)
z.show(10, truncate=False)
spark.stop()
