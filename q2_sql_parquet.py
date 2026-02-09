from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q2_SQL_2015").getOrCreate()

SRC = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2015"

h = 11
L = 4
K = 19

spark.read.parquet(SRC).createOrReplaceTempView("trips")

query = f"""
WITH base AS (
  SELECT
    CAST(VendorID AS INT) AS VendorID,
    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
    tip_amount / trip_distance AS tip_per_dist
  FROM trips
  WHERE
    tpep_pickup_datetime IS NOT NULL
    AND hour(tpep_pickup_datetime) BETWEEN {h} AND {h + (L - 1)}
    AND trip_distance > 0
    AND fare_amount > 0
),
agg AS (
  SELECT
    VendorID,
    pickup_date,
    AVG(tip_per_dist) AS avg_tip_per_dist
  FROM base
  GROUP BY VendorID, pickup_date
),
ranked AS (
  SELECT
    VendorID,
    pickup_date,
    avg_tip_per_dist,
    ROW_NUMBER() OVER (PARTITION BY VendorID ORDER BY avg_tip_per_dist DESC) AS rn
  FROM agg
)
SELECT VendorID, pickup_date, avg_tip_per_dist
FROM ranked
WHERE rn <= {K}
ORDER BY VendorID, avg_tip_per_dist DESC
"""

df = spark.sql(query)

print("=== Q2 SQL EXPLAIN (formatted) ===")
df.explain("formatted")

print("=== Q2 SQL RESULT (top-K per VendorID) ===")
df.show(200, truncate=False)

spark.stop()
