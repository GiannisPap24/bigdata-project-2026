from pyspark.sql import SparkSession

SRC = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2015"
OUT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/csv/yellow_2015_csv"

spark = SparkSession.builder.appName("EXPORT_2015_PARQUET_TO_CSV").getOrCreate()
df = spark.read.parquet(SRC)
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUT)
spark.stop()
