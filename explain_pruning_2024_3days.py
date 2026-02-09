from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour

spark = SparkSession.builder.appName("explain_pruning_2024_3days").getOrCreate()

P = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_part_3days"
df = spark.read.parquet(P)

filtered = (df
    .filter(col("pickup_date").isin("2024-07-09","2024-07-10","2024-07-11"))
    .filter((hour(col("tpep_pickup_datetime")) >= 11) & (hour(col("tpep_pickup_datetime")) <= 14))
)

filtered.explain("formatted")
print("Count:", filtered.count())

spark.stop()
