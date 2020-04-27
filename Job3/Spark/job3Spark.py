import pyspark
import sys


def finalVariance(OpenValue, FinalValue):
    OpenValue = float(OpenValue)
    FinalValue = float(FinalValue)
    return round(((FinalValue - OpenValue) / OpenValue) * 100)


def load_data(Spark):
    lines = Spark.read.csv(sys.argv[1], header=True).rdd.cache()
    return lines


def elaborate(Spark):
    Tickers = load_data(Spark) \
        .filter(lambda x: x[7].split("-")[0] >= '2016') \
        .map(lambda x: ((x[0], x[7].split("-")[0]), [float(x[2])])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[0][0], [(x[0][1], finalVariance(x[1][0], x[1][-1]))])) \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: len(x[1]) == 3) \
        .map(lambda x: (tuple(sorted(x[1], key=lambda y: y[0])), [x[0]])) \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: len(x[1]) > 1) \
        .collect()
    return Tickers


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("Job3Spark").getOrCreate()
    tickers = elaborate(spark)
    for t in tickers:
        print(' '.join(map(str, t[1])) + ":" + ' '.join(map(str, t[0])))
    spark.stop()
