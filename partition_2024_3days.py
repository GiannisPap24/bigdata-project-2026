from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("partition_2024_3days").getOrCreate()

SRC = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024"
OUT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_part_3days"

days = ["2024-07-09", "2024-07-10", "2024-07-11"]

df = spark.read.parquet(SRC)
df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
df = df.filter(col("pickup_date").isin(days))
df = df.repartition(4)

(df.write
   .mode("overwrite")
   .partitionBy("pickup_date")
   .parquet(OUT)
)

spark.stop()
