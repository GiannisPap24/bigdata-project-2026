import argparse
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

def in_window(hour, h, L):
    return (hour >= h) and (hour < h + L)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--k", type=int, default=14)
    p.add_argument("--h", type=int, default=11)
    p.add_argument("--L", type=int, default=4)
    args = p.parse_args()

    spark = SparkSession.builder.appName("Q2_RDD_PARQUET_2015").getOrCreate()

    df = spark.read.parquet(args.input).select(
        F.col("VendorID").cast("int").alias("VendorID"),
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
    ).where(F.col("pickup_ts").isNotNull())

    rdd = df.rdd.map(lambda r: (r["VendorID"], r["pickup_ts"], r["trip_distance"], r["fare_amount"], r["tip_amount"]))

    def mapper(x):
        vendor, pickup_ts, trip_distance, fare_amount, tip_amount = x
        if vendor is None or trip_distance is None or fare_amount is None or tip_amount is None:
            return None
        if trip_distance <= 0 or fare_amount <= 0:
            return None
        try:
            dt = pickup_ts.to_pydatetime() if hasattr(pickup_ts, "to_pydatetime") else pickup_ts
        except:
            dt = pickup_ts
        if not isinstance(dt, datetime):
            return None
        if not in_window(dt.hour, args.h, args.L):
            return None
        d = dt.strftime("%Y-%m-%d")
        tip_per_mile = tip_amount / trip_distance
        return ((vendor, d), (tip_per_mile, 1))

    paired = rdd.map(mapper).filter(lambda x: x is not None)
    summed = paired.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    avgs = summed.mapValues(lambda s: s[0] / s[1])
    by_vendor = avgs.map(lambda x: (x[0][0], (x[1], x[0][1])))

    k = args.k

    def seq(acc, item):
        acc.append(item)
        acc.sort(key=lambda t: t[0], reverse=True)
        if len(acc) > k:
            del acc[k:]
        return acc

    def comb(a, b):
        a.extend(b)
        a.sort(key=lambda t: t[0], reverse=True)
        if len(a) > k:
            del a[k:]
        return a

    topk = by_vendor.aggregateByKey([], seq, comb)
    out = topk.flatMap(lambda kv: [(kv[0], t[1], t[0]) for t in kv[1]]) \
             .sortBy(lambda x: (x[0], -x[2]))

    for row in out.take(200):
        print(f"{row[0]}\t{row[1]}\t{row[2]}")

    spark.stop()

if __name__ == "__main__":
    main()
