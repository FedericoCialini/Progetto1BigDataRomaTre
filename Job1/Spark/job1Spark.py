import pyspark
import sys
from operator import add


def finalVariance(InitiaValue, FinalValue):
    InitiaValue = float(InitiaValue)
    FinalValue = float(FinalValue)
    print(InitiaValue)
    print((FinalValue))
    return str(round(((FinalValue - InitiaValue) / InitiaValue) * 100)) + "%"


spark = pyspark.sql.SparkSession.builder.appName("Job1Spark").getOrCreate()
lines = spark.read.csv(sys.argv[1], header=True).rdd.cache()
listoflists = []
tickers = lines.filter(lambda x: x[7].split("-")[0] >= '2008' and len(x) == 8).map(
    lambda x: (x[0], [[float(x[2]), float(x[4]), float(x[5]), float(x[6])]])).reduceByKey(
    lambda x, y: list(x) + list(y)). \
    map(lambda x: (
x[0], (finalVariance(x[1][0][0], x[1][-1][0]), min(x[1][1]), max(x[1][2]), (sum(x[1][3]) / len(x[1]))))).collect()
print(tickers)

print("----------------------------------------------")
sc = spark.sparkContext
