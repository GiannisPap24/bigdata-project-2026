import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--k", type=int, default=14)
    p.add_argument("--h", type=int, default=11)
    p.add_argument("--L", type=int, default=4)
    args = p.parse_args()

    spark = SparkSession.builder.appName("Q2_DF_2015").getOrCreate()

    df = spark.read.parquet(args.input)

    base = df.select(
        F.col("VendorID").cast("int").alias("VendorID"),
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
    ).where(F.col("pickup_ts").isNotNull()) \
     .withColumn("pickup_hour", F.hour("pickup_ts")) \
     .withColumn("pickup_date", F.to_date("pickup_ts"))

    filtered = base.where((F.col("pickup_hour") >= F.lit(args.h)) & (F.col("pickup_hour") < F.lit(args.h + args.L))) \
        .where((F.col("trip_distance") > 0) & (F.col("fare_amount") > 0)) \
        .withColumn("tip_per_mile", F.col("tip_amount") / F.col("trip_distance"))

    agg = filtered.groupBy("VendorID", "pickup_date").agg(F.avg("tip_per_mile").alias("avg_tip_per_mile"))

    w = Window.partitionBy("VendorID").orderBy(F.col("avg_tip_per_mile").desc())
    out = agg.withColumn("rn", F.row_number().over(w)) \
        .where(F.col("rn") <= args.k) \
        .select("VendorID", "pickup_date", "avg_tip_per_mile") \
        .orderBy(F.col("VendorID").asc(), F.col("avg_tip_per_mile").desc())

    out.show(200, truncate=False)
    out.explain("formatted")

    spark.stop()

if __name__ == "__main__":
    main()
