import pyspark
import sys


def finalVariance(InitiaValue, FinalValue):
    InitiaValue = float(InitiaValue)
    FinalValue = float(FinalValue)
    return round(((FinalValue - InitiaValue) / InitiaValue) * 100)


def takecolumn(x, col):
    List = []
    for s in x:
        List.append(s[col])
    return List


spark = pyspark.sql.SparkSession.builder.appName("Job1Spark").getOrCreate()
lines = spark.read.csv(sys.argv[1], header=True).rdd.cache()
tickers = lines.filter(lambda x: x[7].split("-")[0] >= '2008' and len(x) == 8).map(
    lambda x: (x[0], [[float(x[2]), float(x[4]), float(x[5]), float(x[6])]])).reduceByKey(
    lambda x, y: list(x) + list(y)). \
    map(lambda x: (
    x[0], (finalVariance(x[1][0][0], x[1][-1][0]), min(takecolumn(x[1], 1)), max(takecolumn(x[1], 2)),
           (sum(takecolumn(x[1], 3)) / len(x[1]))))).collect()
tickers = sorted(tickers,key=lambda x : x[1][0],reverse=True )
for i in range(0, len(tickers)):
    print(tickers[i][0], str(tickers[i][1][0]) + "%", tickers[i][1][1], tickers[i][1][2], tickers[i][1][3], sep='\t')
spark.stop()