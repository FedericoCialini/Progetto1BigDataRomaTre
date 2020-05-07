import pyspark
import sys


def finalVariance(OpenValue, FinalValue):
    OpenValue = float(OpenValue)
    FinalValue = float(FinalValue)
    return round(((FinalValue - OpenValue) / OpenValue) * 100)


def load_data(Spark):
    lines = Spark.read.csv(sys.argv[1], header=True).rdd.cache()
    return lines


def load_names(Spark):
    nameDict = {}
    names = Spark.read.csv(sys.argv[2], header=True).rdd.collect()
    for line in names:
        nameDict[line[0]] = line[2]
    return nameDict


def printName(Ticker, NameDict):
    try:
        return NameDict[Ticker]
    except:
        return Ticker


def TakeColumn(Lists, year):
    ColumnList = []
    for List in Lists:
        if List[0] == year:
            ColumnList.append(List[1])
    return ColumnList


def avg(List):
    List = list(map(int, List))
    return str(sum(List) / len(List))


def elaborate(Spark):
    NameDict = load_names(Spark)
    Tickers = load_data(Spark) \
        .filter(lambda x: x[7].split("-")[0] >= '2016') \
        .map(lambda x: ((x[0], x[7].split("-")[0]), [float(x[2])])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[0][0], [(x[0][1], finalVariance(x[1][0], x[1][-1]))])) \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: len(x[1]) == 3) \
        .map(lambda x: (printName(x[0], NameDict), tuple(sorted(x[1], key=lambda y: y[0])))) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: ((avg(TakeColumn(x[1], '2016')), avg(TakeColumn(x[1], '2017')),
                         avg(TakeColumn(x[1], '2018'))), [x[0]])) \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: len(x[1]) > 1) \
        .collect()
    return Tickers, NameDict


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("Job3Spark").getOrCreate()
    tickers, nameDict = elaborate(spark)
    for t in tickers:
        print(';'.join(map(str, t[1]))
              + " : " + '2016 : ' + t[0][0] + "% , " + '2017: ' + t[0][1] + '% , ' + '2018 : ' + t[0][2])
    spark.stop()
