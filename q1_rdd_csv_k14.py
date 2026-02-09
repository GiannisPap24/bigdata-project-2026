import argparse
import csv
import math
from datetime import datetime
from pyspark.sql import SparkSession

def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    return 2*r*math.asin(math.sqrt(a))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--h", type=int, default=11)
    p.add_argument("--L", type=int, default=4)
    args = p.parse_args()

    spark = SparkSession.builder.appName("Q1_RDD_CSV_2015").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(args.input)
    header = rdd.first()
    data = rdd.filter(lambda x: x != header)

    def parse(line):
        try:
            row = next(csv.reader([line]))
        except:
            return None

        try:
            pickup_s = row[1].split("+")[0].replace("T", " ").split(".")[0]
            dropoff_s = row[2].split("+")[0].replace("T", " ").split(".")[0]
            pickup_ts = datetime.strptime(pickup_s, "%Y-%m-%d %H:%M:%S")
            dropoff_ts = datetime.strptime(dropoff_s, "%Y-%m-%d %H:%M:%S")
        except:
            return None


        hour = pickup_ts.hour
        if not (hour >= args.h and hour < args.h + args.L):
            return None

        try:
            trip_distance = float(row[4])
            fare_amount = float(row[12])
            pick_lon = float(row[5])
            pick_lat = float(row[6])
            drop_lon = float(row[9])
            drop_lat = float(row[10])
        except:
            return None

        if trip_distance <= 0 or fare_amount <= 0:
            return None
        if pick_lon == 0 or pick_lat == 0 or drop_lon == 0 or drop_lat == 0:
            return None

        duration_min = (dropoff_ts - pickup_ts).total_seconds() / 60.0
        if duration_min <= 0:
            return None

        hav = haversine_km(pick_lat, pick_lon, drop_lat, drop_lon)
        return (hour, (1, duration_min, hav))

    parsed = data.map(parse).filter(lambda x: x is not None)

    counts = parsed.mapValues(lambda v: v[0]).reduceByKey(lambda a, b: a + b)
    sum_dur = parsed.mapValues(lambda v: v[1]).reduceByKey(lambda a, b: a + b)
    avg_dur = counts.join(sum_dur).mapValues(lambda x: x[1] / x[0])

    hav_sample = parsed.map(lambda x: (x[0], x[1][2])) \
        .sample(False, 0.02, 42) \
        .groupByKey() \
        .mapValues(list)

    def p90(vals):
        if not vals:
            return None
        vals.sort()
        idx = int(math.ceil(0.90 * len(vals))) - 1
        if idx < 0:
            idx = 0
        return vals[idx]

    p90_hav = hav_sample.mapValues(p90)

    base = counts.join(avg_dur).mapValues(lambda x: (x[0], x[1]))

    out = base.leftOuterJoin(p90_hav) \
        .map(lambda kv: (kv[0], kv[1][0][0], kv[1][0][1], kv[1][1])) \
        .sortBy(lambda x: x[0])

    for row in out.collect():
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}")

    spark.stop()

if __name__ == "__main__":
    main()
