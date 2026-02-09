from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, coalesce, lit, greatest, least, round as rnd
import time

spark = SparkSession.builder.appName("Q5_DF").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

h=11; L=4; d=11; K=14
hours=[h,h+1,h+2,h+3]
days=[d,d+1,d+2]

trips_path="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_fixed"
zones_path="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/taxi_zone_lookup"

t = (spark.read.parquet(trips_path)
     .select("PULocationID","DOLocationID","pickup_day","pickup_hour")
     .filter(col("pickup_day").isin(days))
     .filter(col("pickup_hour").isin(hours)))

z = (spark.read.parquet(zones_path)
     .selectExpr("cast(LocationID as int) as LocationID","Borough","Zone"))

pu = z.selectExpr("LocationID as PULocationID","Borough as PUBorough","Zone as PUZone")
do = z.selectExpr("LocationID as DOLocationID","Borough as DOBorough","Zone as DOZone")

tj = t.join(pu, on="PULocationID", how="left").join(do, on="DOLocationID", how="left")

pickups = (tj.groupBy(col("PUBorough").alias("Borough"), col("PUZone").alias("Zone"))
           .agg(count(lit(1)).alias("pickups")))

dropoffs = (tj.groupBy(col("DOBorough").alias("Borough"), col("DOZone").alias("Zone"))
            .agg(count(lit(1)).alias("dropoffs")))

m = (pickups.join(dropoffs, on=["Borough","Zone"], how="full")
     .withColumn("pickups", coalesce(col("pickups"), lit(0)))
     .withColumn("dropoffs", coalesce(col("dropoffs"), lit(0))))

den = greatest(lit(1), least(col("pickups"), col("dropoffs")))
num = greatest(col("pickups"), col("dropoffs"))

res = (m.withColumn("imbalance", num / den)
       .select("Borough","Zone","pickups","dropoffs", rnd(col("imbalance"), 6).alias("imbalance"))
       .orderBy(col("imbalance").desc(), col("pickups").desc(), col("dropoffs").desc())
       .limit(K))

print(f"PARAMS h={h} L={L} d={d} K={K} hours={hours} days={days}")
res.explain("formatted")

t0=time.time()
rows=res.collect()
t1=time.time()

print("RESULTS (top-K imbalance)")
for r in rows:
    print(r)

print(f"elapsed_sec={t1-t0:.2f}")
spark.stop()
