import pyspark
import sys

def create_ticker_sectorPair(spark):
    ticker_sector = historical_stocks(spark) \
        .map(lambda x: (x[0], x[3])) \
        .reduceByKey(lambda x, y: x + y) \
        .collect()
    return ticker_sector

def getSector(sectorPair, ticker):
    sec = "N/A"
    for pair in sectorPair:
        if ticker == pair[0]:
            sec = pair[1]
            break
    return sec

def elaborate(spark, sectorPair):
    Tickers = historical_stock_price(spark)\
        .filter(lambda x: x[7].split("-")[0] >= '2008')\
        .map(lambda x: ((x[0], x[7].split("-")[0]), [[float(x[2]), float(x[6])]]))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: ((getSector(sectorPair, x[0][0]), x[0][1]), [[finalVariance(x[1][0][0], x[1][-1][0]), avgColumn(x[1], 0), sum(TakeColumn(x[1], 1))]]))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0], round(avgColumn(x[1], 0), 2), avgColumn(x[1], 1), avgColumn(x[1], 2)))\
        .collect()
    Tickers = sorted(Tickers, key=lambda x: x[0], reverse=False)
    return Tickers

def historical_stock_price(Spark):
    lines1 = Spark.read.csv(sys.argv[1], header=True).rdd.cache()
    return lines1

def historical_stocks(Spark):
    lines = Spark.read.csv(sys.argv[2], header=True).rdd.cache()
    return lines

def finalVariance(OpenValue, FinalValue):
    OpenValue = float(OpenValue)
    FinalValue = float(FinalValue)
    return ((FinalValue - OpenValue) / OpenValue) * 100

def TakeColumn(Lists, col):
    ColumnList = []
    for List in Lists:
        ColumnList.append(List[col])
    return ColumnList

def avgColumn(Lists, col):
    return sum(TakeColumn(Lists, col))/len(Lists)

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("Job2Spark").getOrCreate()
    tickers = elaborate(spark, create_ticker_sectorPair(spark))
    print(
        f'{"SETTORE":30} | {"ANNO":30} | {"VARIAZIONE ANNUALE MEDIA":30} | {"QUOTAZIONE MEDIA GIORNALIERA":30} | {"VOLUME MEDIO ANNUALE ":30} \n ')
    for i in range(0, len(tickers)):
        print(f'{tickers[i][0][0]:30} | {tickers[i][0][1]:30} | {str(tickers[i][1]):30} | {str(tickers[i][2]):30} | {str(tickers[i][3]):25} ')
    spark.stop()
