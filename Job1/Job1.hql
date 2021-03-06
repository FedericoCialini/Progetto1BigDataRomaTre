set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS tickers;
DROP TABLE IF EXISTS names;
DROP TABLE IF EXISTS tickeryear;
DROP TABLE IF EXISTS minyears;
DROP TABLE IF EXISTS maxyears;


CREATE TABLE IF NOT EXISTS tickers (ticker STRING, openvalues FLOAT, closevalue FLOAT,adjustedThe FLOAT,low FLOAT,high FLOAT,volume FLOAT,day DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "skip.header.line.count"="1")
   STORED AS TEXTFILE
LOCATION  '/home/federico/PycharmProjects/progetto1BigData/daily-historical-stock-prices-1970-2018/new_historical_stock_prices_double.csv';
LOAD DATA LOCAL INPATH '/home/federico/PycharmProjects/progetto1BigData/daily-historical-stock-prices-1970-2018/new_historical_stock_prices_double.csv'
OVERWRITE INTO TABLE tickers;


CREATE TABLE IF NOT EXISTS tickeryear AS
SELECT
    ticker,MIN(TO_DATE(day)) AS mindata,MAX(TO_DATE(day)) AS maxdata,min(closevalue) as minclose,max(closevalue) as maxclose,avg(volume) as avgvolume
FROM tickers
WHERE year(day)>=2008
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS minyears AS
SELECT
    t.ticker,
    t.closevalue as minvalue
FROM tickeryear y JOIN tickers t ON t.ticker=y.ticker
WHERE (t.day = y.mindata);

CREATE TABLE IF NOT EXISTS maxyears AS
SELECT
    t.ticker,
    t.closevalue as maxvalue
FROM tickeryear y  JOIN tickers t ON t.ticker=y.ticker
WHERE (t.day = y.maxdata);


SELECT
    b.ticker,
    round((((b.maxvalue-a.minvalue)/a.minvalue) * 100)) AS percentagevariation,
    y.minclose,
    y.maxclose,
    y.avgvolume
FROM  minyears a JOIN maxyears b JOIN tickeryear y  ON a.ticker = b.ticker and a.ticker = y.ticker
SORT BY percentagevariation DESC
LIMIT 10;









