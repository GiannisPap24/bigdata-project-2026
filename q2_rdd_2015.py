from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date
from pyspark.sql.types import DoubleType
from datetime import date

K = 19

spark = SparkSession.builder.appName("Q2_RDD_2015").getOrCreate()

SRC = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/ioanpapadopoulos/data/parquet_v2/yellow_2015"

df = (
    spark.read.parquet(SRC)
    .select("VendorID", "tpep_pickup_datetime", "trip_distance", "fare_amount", "tip_amount")
    .where(col("tpep_pickup_datetime").isNotNull())
    .where(col("trip_distance").isNotNull() & col("fare_amount").isNotNull())
    .where((col("trip_distance") > 0) & (col("fare_amount") > 0))
    .where((hour(col("tpep_pickup_datetime")) >= 11) & (hour(col("tpep_pickup_datetime")) <= 14))
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
)

rdd = df.rdd.map(lambda r: (
    int(r["VendorID"]) if r["VendorID"] is not None else None,
    r["pickup_date"],
    float(r["tip_amount"]) / float(r["trip_distance"]) if r["trip_distance"] else None
))


rdd = rdd.filter(lambda x: x[0] is not None and x[1] is not None and x[2] is not None)

pair = rdd.map(lambda x: ((x[0], x[1]), (x[2], 1)))
agg = pair.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
avg = agg.map(lambda kv: (kv[0][0], kv[0][1], kv[1][0] / kv[1][1]))  


grouped = avg.map(lambda x: (x[0], (x[1], x[2]))).groupByKey()

def topk(values, k=K):
    vals = list(values)
    vals.sort(key=lambda t: t[1], reverse=True)
    return vals[:k]

top = grouped.flatMap(lambda kv: [(kv[0], d, v) for (d, v) in topk(kv[1])])

out = top.sortBy(lambda x: (x[0], str(x[1]), -x[2]))

print("VendorID|pickup_date|avg_tip_per_dist")
for v, d, a in out.collect():
    print(f"{v}|{d}|{a}")

spark.stop()
