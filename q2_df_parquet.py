from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, avg, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Q2_DF_2015").getOrCreate()

SRC = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2015"

h = 11
L = 4
K = 19

df = spark.read.parquet(SRC)

df = (df
  .filter((hour(col("tpep_pickup_datetime")) >= h) & (hour(col("tpep_pickup_datetime")) <= h + (L - 1)))
  .filter(col("trip_distance") > 0)
  .filter(col("fare_amount") > 0)
  .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
  .withColumn("tip_per_dist", col("tip_amount") / col("trip_distance"))
)

g = df.groupBy("VendorID", "pickup_date").agg(avg(col("tip_per_dist")).alias("avg_tip_per_dist"))

w = Window.partitionBy("VendorID").orderBy(desc("avg_tip_per_dist"))
res = g.withColumn("rn", row_number().over(w)).filter(col("rn") <= K).drop("rn")

print("=== Q2 DF EXPLAIN (formatted) ===")
res.explain("formatted")

print("=== Q2 DF RESULT (top-K per VendorID) ===")
res.orderBy("VendorID", desc("avg_tip_per_dist")).show(200, truncate=False)

spark.stop()
