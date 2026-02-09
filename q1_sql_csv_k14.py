import argparse
from pyspark.sql import SparkSession

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--h", type=int, default=11)
    p.add_argument("--L", type=int, default=4)
    args = p.parse_args()

    spark = SparkSession.builder.appName("Q1_SQL_CSV_2015").getOrCreate()

    df = spark.read.option("header", "true").csv(args.input, inferSchema=True)
    df.createOrReplaceTempView("trips")

    h = args.h
    L = args.L

    q = f"""
    WITH base AS (
      SELECT
        tpep_pickup_datetime AS pickup_ts,
        tpep_dropoff_datetime AS dropoff_ts,
        trip_distance,
        fare_amount,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude
      FROM trips
      WHERE tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
    ),
    filtered AS (
      SELECT
        hour(pickup_ts) AS pickup_hour,
        (unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60.0 AS duration_min,
        2 * 6371 * asin(sqrt(
          pow(sin(radians((dropoff_latitude - pickup_latitude) / 2)), 2) +
          cos(radians(pickup_latitude)) * cos(radians(dropoff_latitude)) *
          pow(sin(radians((dropoff_longitude - pickup_longitude) / 2)), 2)
        )) AS hav_km
      FROM base
      WHERE hour(pickup_ts) >= {h} AND hour(pickup_ts) < {h+L}
        AND trip_distance IS NOT NULL AND trip_distance > 0
        AND fare_amount IS NOT NULL AND fare_amount > 0
        AND pickup_longitude IS NOT NULL AND pickup_longitude != 0
        AND pickup_latitude  IS NOT NULL AND pickup_latitude  != 0
        AND dropoff_longitude IS NOT NULL AND dropoff_longitude != 0
        AND dropoff_latitude  IS NOT NULL AND dropoff_latitude  != 0
    )
    SELECT
      pickup_hour,
      count(*) AS trips,
      avg(duration_min) AS avg_duration_min,
      percentile_approx(hav_km, 0.90) AS p90_haversine_km
    FROM filtered
    WHERE duration_min > 0
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """

    out = spark.sql(q)
    out.show(50, truncate=False)
    out.explain("formatted")

    spark.stop()

if __name__ == "__main__":
    main()
