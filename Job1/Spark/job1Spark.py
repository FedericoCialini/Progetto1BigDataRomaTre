import pyspark
import sys


def finalVariance(OpenValue, FinalValue):
    OpenValue = float(OpenValue)
    FinalValue = float(FinalValue)
    return round(((FinalValue - OpenValue) / OpenValue) * 100)


def TakeColumn(Lists, col):
    ColumnList = []
    for List in Lists:
        ColumnList.append(List[col])
    return ColumnList


def load_data(Spark):
    lines = Spark.read.csv(sys.argv[1], header=True).rdd.cache()
    return lines


def elaborate(Spark):
    Tickers = load_data(Spark) \
        .filter(lambda x: x[7].split("-")[0] >= '2008') \
        .map(lambda x: (x[0], [(float(x[2]), float(x[6]))])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[0], (finalVariance(x[1][0][0], x[1][-1][0]), min(TakeColumn(x[1], 0)),
                               max(TakeColumn(x[1], 0)), (sum(TakeColumn(x[1], 1)) / len(x[1]))))) \
        .collect()
    Tickers = sorted(Tickers, key=lambda x: x[1][0], reverse=True)
    return Tickers


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("Job1Spark").getOrCreate()
    tickers = elaborate(spark)
    for i in range(0, len(tickers)):
        print(tickers[i][0], str(tickers[i][1][0]) + "%", tickers[i][1][1], tickers[i][1][2], tickers[i][1][3],
              sep='\t')
    spark.stop()
