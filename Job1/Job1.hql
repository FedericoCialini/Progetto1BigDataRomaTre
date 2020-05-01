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




CREATE TABLE IF NOT EXISTS tickeryear AS
SELECT
    ticker,MIN(TO_DATE(day)) AS mindata,MAX(TO_DATE(day)) AS maxdata,min(closevalue) as minclose,max(closevalue) as maxclose,avg(volume) as avgvolume
FROM tickers
WHERE year(day)>=2008
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS minyears AS
SELECT
    t.ticker,
    t.closevalue as minvalue,
FROM tickers t JOIN tickeryear y ON t.ticker=y.ticker
WHERE (t.day = y.mindata);

CREATE TABLE IF NOT EXISTS maxyears AS
SELECT
    t.ticker,
    t.closevalue as maxvalue,
FROM tickers t JOIN tickeryear y ON t.ticker=y.ticker
WHERE (t.day = y.maxdata);


SELECT
    b.ticker,
    (((b.maxvalue-a.minvalue)/a.minvalue) * 100) AS percentagevariation,
    y.minclose,
    y.maxclose,
    y.avgvolume
FROM  minyears a JOIN maxyears b JOIN tickeryear y  ON a.ticker = b.ticker and a.ticker = y.ticker
ORDER BY percentagevariation DESC;









