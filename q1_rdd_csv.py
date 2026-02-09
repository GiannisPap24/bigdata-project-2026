from pyspark.sql import SparkSession
import math, time
from datetime import datetime

A = 2121035
h = A % 24
L = 4
HOURS = set([(h + i) % 24 for i in range(L)])

CSV2015 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2015.csv"
SAMPLE_K = 50000

def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(a))

def in_bounds(lat, lon):
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0

def parse_line(line):
    parts = line.split(",")
    if len(parts) < 19:
        return None
    if parts[0] == "VendorID":
        return None
    try:
        pu = datetime.strptime(parts[1], "%Y-%m-%d %H:%M:%S")
        do = datetime.strptime(parts[2], "%Y-%m-%d %H:%M:%S")
        hr = pu.hour
        if hr not in HOURS:
            return None
        dur = (do - pu).total_seconds() / 60.0
        if dur <= 0:
            return None

        plon = float(parts[5]); plat = float(parts[6])
        dlon = float(parts[9]); dlat = float(parts[10])

        if plon == 0.0 or plat == 0.0 or dlon == 0.0 or dlat == 0.0:
            return None
        if not (in_bounds(plat, plon) and in_bounds(dlat, dlon)):
            return None

        dist = haversine_km(plat, plon, dlat, dlon)
        return (hr, dur, dist)
    except:
        return None

def comb_create(v):
    c, s = v
    return (c, s)

def comb_merge_value(a, v):
    c1, s1 = a
    c2, s2 = v
    return (c1 + c2, s1 + s2)

def comb_merge_combiners(a, b):
    return (a[0] + b[0], a[1] + b[1])

def samp_create(x):
    return (1, [x])

def samp_merge_value(state, x):
    n, arr = state
    n2 = n + 1
    if len(arr) < SAMPLE_K:
        arr.append(x)
    else:
        j = (n2 * 1103515245 + 12345) & 0x7fffffff
        j = j % n2
        if j < SAMPLE_K:
            arr[j] = x
    return (n2, arr)

def samp_merge_combiners(a, b):
    n1, s1 = a
    n2, s2 = b
    s = s1 + s2
    n = n1 + n2
    if len(s) > SAMPLE_K:
        step = max(1, len(s) // SAMPLE_K)
        s = s[::step][:SAMPLE_K]
    return (n, s)

def p90(vals):
    if not vals:
        return None
    vals.sort()
    idx = int(math.ceil(0.90 * len(vals))) - 1
    if idx < 0:
        idx = 0
    if idx >= len(vals):
        idx = len(vals) - 1
    return vals[idx]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1-RDD-CSV").getOrCreate()
    sc = spark.sparkContext

    base = sc.textFile(CSV2015).map(parse_line).filter(lambda x: x is not None).cache()

    t0 = time.time()
    agg = base.map(lambda x: (x[0], (1, x[1]))).combineByKey(comb_create, comb_merge_value, comb_merge_combiners)
    samp = base.map(lambda x: (x[0], x[2])).combineByKey(samp_create, samp_merge_value, samp_merge_combiners)

    agg_map = dict(agg.collect())
    samp_map = dict(samp.collect())

    rows = []
    for hr in sorted(HOURS):
        if hr not in agg_map:
            continue
        trips, sumdur = agg_map[hr]
        avgdur = sumdur / trips
        dists = samp_map.get(hr, (0, []))[1]
        rows.append((hr, trips, avgdur, p90(dists)))

    t1 = time.time()

    print("A", A, "h", h, "L", L, "hours", sorted(HOURS))
    print("hour Trips AvgDurationMin P90HaversineKm")
    for r in rows:
        print(r[0], r[1], r[2], r[3])
    print("elapsed_sec_cold", t1 - t0)

    t2 = time.time()
    _ = agg.count()
    _ = samp.count()
    t3 = time.time()
    print("elapsed_sec_warm", t3 - t2)

    spark.stop()

