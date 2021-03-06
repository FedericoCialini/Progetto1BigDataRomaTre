set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS tickers;
DROP TABLE IF EXISTS names;
DROP TABLE IF EXISTS dates;
DROP TABLE IF EXISTS MinMaxDates;
DROP TABLE IF EXISTS OpenValues;
DROP TABLE IF EXISTS FinalValues;
DROP TABLE IF EXISTS PercentageVariations;
DROP TABLE IF EXISTS SamePercentageYear;
DROP TABLE IF EXISTS VariationsPerTicker;
DROP TABLE IF EXISTS SameVariations;
DROP TABLE IF EXISTS checkYears;


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
CREATE TABLE IF NOT EXISTS names (ticker STRING,exc STRING, name STRING,sector STRING,industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "skip.header.line.count"="1")
   STORED AS TEXTFILE
LOCATION  '/home/federico/PycharmProjects/progetto1BigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv';
LOAD DATA LOCAL INPATH '/home/federico/PycharmProjects/progetto1BigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv'
OVERWRITE INTO TABLE names;


CREATE TABLE IF NOT EXISTS dates as 
SELECT ticker,closevalue,day
FROM tickers 
WHERE YEAR(day)>='2016';

CREATE TABLE IF NOT EXISTS MinMaxDates as 
SELECT ticker,min(day) as mindate,max(day) as maxdate,year(day) as anno
FROM dates
GROUP BY ticker,year(day);


CREATE TABLE IF NOT EXISTS OpenValues AS 
SELECT a.ticker,a.closevalue as openvalue,year(a.day) as mindate
FROM dates a JOIN MinMaxDates b ON a.ticker = b.ticker AND a.day = b.mindate;

CREATE TABLE IF NOT EXISTS FinalValues AS 
SELECT a.ticker,a.closevalue as finalvalue,year(a.day) as maxdate
FROM dates a JOIN MinMaxDates b on a.ticker = b.ticker and a.day = b.maxdate;

CREATE TABLE IF NOT EXISTS checkYears AS 
SELECT n.name, a.mindate AS anno,ROUND(((b.finalvalue - a.openvalue)/a.openvalue)*100) as variation
FROM (OpenValues a JOIN FinalValues b ON a.ticker = b.ticker AND a.mindate = b.maxdate) JOIN names n ON a.ticker = n.ticker
WHERE a.ticker IN (SELECT ticker FROM OpenValues GROUP BY ticker HAVING COUNT(*) = 3);

CREATE TABLE IF NOT EXISTS PercentageVariations AS
SELECT name,avg(variation) as yearvariation
FROM checkYears
GROUP BY name,anno;

CREATE TABLE IF NOT EXISTS VariationsPerTicker AS
SELECT name, collect_list(yearvariation) AS percentages
FROM PercentageVariations
GROUP BY name;

CREATE TABLE IF NOT EXISTS SameVariations AS
SELECT percentages, COLLECT_LIST(a.name) AS namelist
FROM VariationsPerTicker a
WHERE size(percentages) = 3
GROUP BY percentages;

SELECT percentages,namelist 
FROM SameVariations
WHERE size(namelist) > 1
LIMIT 10;
