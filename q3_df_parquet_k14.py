from pyspark.sql import SparkSession
from pyspark.sql import functions as F

A = 2121035
h = A % 24
L = 4
d = (A % 25) + 1
K = (A % 11) + 10

hours = [(h + i) % 24 for i in range(L)]
days = [x for x in [d, d + 1, d + 2] if x <= 28]

PARQUET2024 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2024_part_3days"
TAXI_ZONES  = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/taxi_zone_lookup"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q3-DF-PARQUET").getOrCreate()

    trips = spark.read.parquet(PARQUET2024)
    zones = spark.read.parquet(TAXI_ZONES).select(
        F.col("LocationID").alias("LocationID"),
        F.col("Borough").alias("Borough")
    )

    trips_f = trips.filter(
        F.dayofmonth(F.col("tpep_pickup_datetime")).isin(days) &
        F.hour(F.col("tpep_pickup_datetime")).isin(hours) &
        F.col("PULocationID").isNotNull() &
        F.col("DOLocationID").isNotNull()
    )

    pu = zones.alias("pu")
    do = zones.alias("do")
    t  = trips_f.alias("t")

    joined = (
        t.join(pu, F.col("t.PULocationID") == F.col("pu.LocationID"), "inner")
         .join(do, F.col("t.DOLocationID") == F.col("do.LocationID"), "inner")
         .select(
             F.col("pu.Borough").alias("PickupBorough"),
             F.col("do.Borough").alias("DropoffBorough")
         )
         .filter(
             F.col("PickupBorough").isNotNull() &
             F.col("DropoffBorough").isNotNull() &
             (F.col("PickupBorough") != F.col("DropoffBorough"))
         )
    )

    res = (
        joined.groupBy("PickupBorough", "DropoffBorough")
              .agg(F.count("*").alias("Trips"))
              .orderBy(F.col("Trips").desc(), F.col("PickupBorough"), F.col("DropoffBorough"))
              .limit(K)
    )

    print(f"A={A} h={h} L={L} hours={hours} d={d} days={days} K={K}")
    res.show(K, truncate=False)

    spark.stop()
