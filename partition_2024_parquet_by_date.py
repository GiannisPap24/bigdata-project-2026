from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("partition_2024_parquet_by_date").getOrCreate()

SRC = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024"
OUT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_part"

df = spark.read.parquet(SRC)

# create partition column from timestamp
df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
df = df.repartition(8)

# write partitioned parquet
(df.write
   .mode("overwrite")
   .partitionBy("pickup_date")
   .parquet(OUT)
)

spark.stop()
