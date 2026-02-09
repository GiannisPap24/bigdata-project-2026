import argparse
from pyspark.sql import SparkSession

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--k", type=int, default=14)
    p.add_argument("--h", type=int, default=11)
    p.add_argument("--L", type=int, default=4)
    args = p.parse_args()

    spark = SparkSession.builder.appName("Q2_SQL_2015").getOrCreate()

    src = args.input
    h = args.h
    L = args.L
    k = args.k

    df = spark.read.parquet(src)
    df.createOrReplaceTempView("trips")

    query = f"""
    WITH base AS (
      SELECT
        VendorID,
        CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
        hour(tpep_pickup_datetime) AS pickup_hour,
        tip_amount,
        trip_distance,
        fare_amount
      FROM trips
      WHERE tpep_pickup_datetime IS NOT NULL
    ),
    filtered AS (
      SELECT
        VendorID,
        pickup_date,
        (tip_amount / trip_distance) AS tip_per_mile
      FROM base
      WHERE pickup_hour >= {h} AND pickup_hour < {h + L}
        AND trip_distance > 0
        AND fare_amount > 0
    ),
    agg AS (
      SELECT
        VendorID,
        pickup_date,
        avg(tip_per_mile) AS avg_tip_per_mile
      FROM filtered
      GROUP BY VendorID, pickup_date
    ),
    ranked AS (
      SELECT
        VendorID,
        pickup_date,
        avg_tip_per_mile,
        row_number() OVER (PARTITION BY VendorID ORDER BY avg_tip_per_mile DESC) AS rn
      FROM agg
    )
    SELECT VendorID, pickup_date, avg_tip_per_mile
    FROM ranked
    WHERE rn <= {k}
    ORDER BY VendorID ASC, avg_tip_per_mile DESC
    """

    out = spark.sql(query)
    out.show(200, truncate=False)
    out.explain("formatted")

    spark.stop()

if __name__ == "__main__":
    main()
