import argparse
import csv
from datetime import datetime
from pyspark.sql import SparkSession

def in_window(hour, h, L):
    return (hour >= h) and (hour < h + L)

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def safe_int(x):
    try:
        return int(x)
    except:
        return None

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--k", type=int, default=14)
    p.add_argument("--h", type=int, default=11)
    p.add_argument("--L", type=int, default=4)
    args = p.parse_args()

    spark = SparkSession.builder.appName("Q2_RDD_2015").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(args.input)
    header = rdd.first()
    data = rdd.filter(lambda x: x != header)

    def mapper(line):
        try:
            row = next(csv.reader([line]))
        except:
            return None
        vendor = safe_int(row[0])
        pickup = row[1]
        trip_distance = safe_float(row[4])
        fare_amount = safe_float(row[10])
        tip_amount = safe_float(row[13])
        if vendor is None or trip_distance is None or fare_amount is None or tip_amount is None:
            return None
        if trip_distance <= 0 or fare_amount <= 0:
            return None
        try:
            dt = datetime.strptime(pickup, "%Y-%m-%d %H:%M:%S")
        except:
            return None
        if not in_window(dt.hour, args.h, args.L):
            return None
        d = dt.strftime("%Y-%m-%d")
        tip_per_mile = tip_amount / trip_distance
        return ((vendor, d), (tip_per_mile, 1))

    paired = data.map(mapper).filter(lambda x: x is not None)
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

    for row in out.collect():
        print(f"{row[0]}\t{row[1]}\t{row[2]}")

    spark.stop()

if __name__ == "__main__":
    main()
