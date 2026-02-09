from pyspark.sql import SparkSession

PARQUET2024 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_part_3days"

spark = SparkSession.builder.appName("CHECK-SCHEMA-Q3").getOrCreate()
df = spark.read.parquet(PARQUET2024)

print("COLUMNS:", df.columns)
df.printSchema()

spark.stop()
