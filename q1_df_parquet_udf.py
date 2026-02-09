from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, udf, expr, percentile_approx
from pyspark.sql.types import DoubleType
import math

spark = SparkSession.builder.appName("q1_df_parquet_udf").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

P2015 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2015"

def haversine_km(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    # basic guard (avoid weird zeros already filtered, but keep safe)
    if lat1 == 0 or lon1 == 0 or lat2 == 0 or lon2 == 0:
        return None
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

haversine_udf = udf(haversine_km, DoubleType())

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

df = df.withColumn(
    "haversine_km",
    haversine_udf(col("pickup_latitude"), col("pickup_longitude"), col("dropoff_latitude"), col("dropoff_longitude"))
)

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
