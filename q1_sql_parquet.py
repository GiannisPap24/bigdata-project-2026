from pyspark.sql import SparkSession
import time

A = 2121035
h = A % 24
L = 4
hours = [(h + i) % 24 for i in range(L)]
hours_sql = ",".join([str(x) for x in hours])

PARQUET2015 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2015"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1-SQL-PARQUET").getOrCreate()

    df = spark.read.parquet(PARQUET2015)
    df.createOrReplaceTempView("p2015")

    query = f"""
    WITH base AS (
      SELECT
        hour(tpep_pickup_datetime) AS pickup_hour,
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0 AS duration_min,
        CAST(pickup_latitude AS DOUBLE) AS plat,
        CAST(pickup_longitude AS DOUBLE) AS plon,
        CAST(dropoff_latitude AS DOUBLE) AS dlat,
        CAST(dropoff_longitude AS DOUBLE) AS dlon
      FROM p2015
    ),
    filtered AS (
      SELECT *
      FROM base
      WHERE pickup_hour IN ({hours_sql})
        AND duration_min > 0
        AND plat IS NOT NULL AND plon IS NOT NULL AND dlat IS NOT NULL AND dlon IS NOT NULL
        AND plat <> 0 AND plon <> 0 AND dlat <> 0 AND dlon <> 0
        AND plat BETWEEN -90 AND 90 AND dlat BETWEEN -90 AND 90
        AND plon BETWEEN -180 AND 180 AND dlon BETWEEN -180 AND 180
    ),
    with_dist AS (
      SELECT
        pickup_hour,
        duration_min,
        2 * 6371.0 * asin(sqrt(
          pow(sin(radians(dlat - plat)/2), 2) +
          cos(radians(plat)) * cos(radians(dlat)) * pow(sin(radians(dlon - plon)/2), 2)
        )) AS haversine_km
      FROM filtered
    )
    SELECT
      pickup_hour AS hour,
      count(*) AS Trips,
      avg(duration_min) AS AvgDurationMin,
      percentile_approx(haversine_km, 0.9) AS P90HaversineKm
    FROM with_dist
    GROUP BY pickup_hour
    ORDER BY hour
    """

    print("A", A, "h", h, "L", L, "hours", hours)

    t0 = time.time()
    res = spark.sql(query).collect()
    t1 = time.time()

    print("hour Trips AvgDurationMin P90HaversineKm")
    for r in res:
        print(r["hour"], r["Trips"], r["AvgDurationMin"], r["P90HaversineKm"])
    print("elapsed_sec_cold", t1 - t0)

    t2 = time.time()
    _ = spark.sql(query).collect()
    t3 = time.time()
    print("elapsed_sec_warm", t3 - t2)

    spark.stop()
