from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, expr, percentile_approx

spark = SparkSession.builder.appName("q1_df_parquet_noudf").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

P2015 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2015"

df = spark.read.parquet(P2015)

df = (
    df
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn("duration_min", (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0)
    .filter(col("pickup_hour").between(11, 14))
    .filter(col("duration_min") > 0)
    .filter(
        (col("pickup_longitude") != 0) & (col("pickup_latitude") != 0) &
        (col("dropoff_longitude") != 0) & (col("dropoff_latitude") != 0)
    )
)

haversine_km = expr("""
2 * 6371 * asin(
  sqrt(
    pow(sin(radians(dropoff_latitude - pickup_latitude) / 2), 2) +
    cos(radians(pickup_latitude)) * cos(radians(dropoff_latitude)) *
    pow(sin(radians(dropoff_longitude - pickup_longitude) / 2), 2)
  )
)
""")

df = df.withColumn("haversine_km", haversine_km)

res = (
    df.groupBy("pickup_hour")
      .agg(
          expr("count(1) as Trips"),
          expr("avg(duration_min) as AvgDurationMin"),
          percentile_approx(col("haversine_km"), 0.9).alias("P90HaversineKm")
      )
      .orderBy("pickup_hour")
)

res.explain("formatted")
res.show(50, truncate=False)
import time
time.sleep(120)

spark.stop()
