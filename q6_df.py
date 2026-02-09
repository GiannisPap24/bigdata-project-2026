from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = SparkSession.builder.getOrCreate()

base = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
trips_path = base + "/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_fixed"
zones_path = base + "/user/ioanpapadopoulos/data/parquet_v2/taxi_zone_lookup"

days = [11, 12, 13]
hours = [11, 12, 13, 14]

trips_raw = spark.read.parquet(trips_path)

airport_col = "airport_fee" if "airport_fee" in trips_raw.columns else "Airport_fee"

trips = (
    trips_raw
    .where(F.col("pickup_day").isin(days) & F.col("pickup_hour").isin(hours))
    .select("VendorID", "PULocationID", "total_amount", "congestion_surcharge", airport_col)
    .withColumn("total_amount", F.coalesce(F.col("total_amount").cast("double"), F.lit(0.0)))
    .withColumn("congestion_surcharge", F.coalesce(F.col("congestion_surcharge").cast("double"), F.lit(0.0)))
    .withColumn("airport_fee", F.coalesce(F.col(airport_col).cast("double"), F.lit(0.0)))
)

zones = (
    spark.read.parquet(zones_path)
    .select(F.col("LocationID").alias("PULocationID"), F.col("service_zone"))
)

df = (
    trips.join(zones, "PULocationID", "inner")
    .withColumn("CongestionAirport_part", F.col("congestion_surcharge") + F.col("airport_fee"))
    .groupBy("VendorID", "service_zone")
    .agg(
        F.count("*").alias("Trips"),
        F.sum("total_amount").alias("TotalRevenue"),
        F.sum("CongestionAirport_part").alias("CongestionAirport")
    )
    .withColumn(
        "Share",
        F.when(F.col("TotalRevenue") > 0, F.col("CongestionAirport") / F.col("TotalRevenue")).otherwise(F.lit(0.0))
    )
    .withColumn("AvgRevenuePerTrip", F.col("TotalRevenue") / F.col("Trips"))
    .orderBy("VendorID", "service_zone")
)

print("master =", spark.sparkContext.master)
df.explain("formatted")

t0 = time.time()
rows = df.collect()
t1 = time.time()

print("elapsed_sec =", round(t1 - t0, 2))
for r in rows:
    print(r)

spark.stop()
