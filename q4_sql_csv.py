from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, dayofmonth, hour, col
import time

spark = SparkSession.builder.appName("Q4_SQL_CSV").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

h=11; L=4; d=11; K=14
hours=[h,h+1,h+2,h+3]
days=[d,d+1,d+2]

trips_path="hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv"
zones_path="hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv"

raw = spark.read.option("header","true").csv(trips_path)
fmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
t = (raw
     .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"), fmt))
     .withColumn("pickup_day", dayofmonth(col("pickup_ts")))
     .withColumn("pickup_hour", hour(col("pickup_ts")))
     .selectExpr(
         "cast(PULocationID as int) as PULocationID",
         "cast(payment_type as int) as payment_type",
         "cast(fare_amount as double) as fare_amount",
         "cast(tip_amount as double) as tip_amount",
         "cast(pickup_day as int) as pickup_day",
         "cast(pickup_hour as int) as pickup_hour"
     )
)

zraw = spark.read.option("header","true").csv(zones_path)
z = zraw.selectExpr("cast(LocationID as int) as LocationID", "Borough")

t.createOrReplaceTempView("trips")
z.createOrReplaceTempView("zones")

query = f"""
SELECT
  z.Borough AS PickupBorough,
  COUNT(*) AS Trips,
  AVG(CASE WHEN t.payment_type=1 THEN 1.0 ELSE 0.0 END) AS card_share,
  AVG(CASE WHEN t.payment_type=1 AND t.fare_amount>0 THEN t.tip_amount/t.fare_amount END) AS avg_tip_rate_card
FROM trips t
JOIN zones z ON t.PULocationID = z.LocationID
WHERE t.pickup_day IN ({",".join(map(str,days))})
  AND t.pickup_hour IN ({",".join(map(str,hours))})
GROUP BY z.Borough
ORDER BY avg_tip_rate_card DESC
"""

df = spark.sql(query)

print(f"PARAMS h={h} L={L} d={d} K={K} hours={hours} days={days}")
df.explain("formatted")

t0=time.time()
rows=df.collect()
t1=time.time()

print("RESULTS")
for r in rows:
    print(r)

print(f"elapsed_sec={t1-t0:.2f}")
spark.stop()
