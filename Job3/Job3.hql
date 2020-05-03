set hive.auto.convert.join = false;
set mapred.compress.map.output=true
set hive.exec.parallel=true

CREATE TABLE IF NOT EXISTS tickers (ticker STRING, openvalues FLOAT, closevalue FLOAT,adjustedThe FLOAT,low FLOAT,high FLOAT,volume FLOAT,day DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION  './data/historical_stock_prices.csv'
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH './data/historical_stock_prices.csv'
OVERWRITE INTO TABLE tickers;

CREATE TABLE IF NOT EXISTS names (ticker STRING,exc STRING, name STRING,sector STRING,industry STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION  './data/historical_stocks.csv'
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH './data/historical_stocks.csv'
OVERWRITE INTO TABLE names;

DROP TABLE IF EXISTS dates;
DROP TABLE IF EXISTS MinMaxDates;
DROP TABLE IF EXISTS OpenValues;
DROP TABLE IF EXISTS CloseValues;
DROP TABLE IF EXISTS PercentageVariations;
DROP TABLE IF EXISTS SamePercentageYear;
DROP TABLE IF EXISTS VariationsPerTicker;
DROP TABLE IF EXISTS SameVariations;

CREATE TABLE IF NOT EXISTS dates as
SELECT ticker,closevalue,day
FROM tickers 
WHERE YEAR(day)>='2016';

CREATE TABLE IF NOT EXISTS MinMaxDates as
SELECT ticker,min(day) as mindate,max(day) as maxdate,year(day) as anno
FROM dates
GROUP BY ticker,year(day);


CREATE TABLE IF NOT EXISTS OpenValues AS
SELECT a.ticker,a.closevalue,year(a.day) as anno
FROM dates a JOIN MinMaxDates b ON a.ticker = b.ticker AND a.day = b.mindate;

CREATE TABLE IF NOT EXISTS CloseValues AS
SELECT a.ticker,a.closevalue,year(a.day) as anno
FROM dates a JOIN MinMaxDates b on a.ticker = b.ticker and a.day = b.maxdate;

CREATE TABLE IF NOT EXISTS PercentageVariations AS
SELECT
    o.ticker,
    ROUND((((c.closevalue-o.closevalue)/o.closevalue) * 100)) AS percentagevariation,
    o.anno
FROM OpenValues o JOIN CloseValues c ON o.ticker = c.ticker AND o.anno = c.anno;

CREATE TABLE IF NOT EXISTS VariationsPerTicker AS
SELECT ticker, collect_list(percentagevariation) AS percentages
FROM PercentageVariations
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS SameVariations AS
SELECT percentages, COLLECT_LIST(b.name) AS namelist
FROM VariationsPerTicker a JOIN names b ON a.ticker = b.ticker
WHERE size(percentages) = 3
GROUP BY percentages;

SELECT *
FROM SameVariations
WHERE size(namelist) > 1;









