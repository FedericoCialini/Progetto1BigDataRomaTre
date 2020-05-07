CREATE TABLE IF NOT EXISTS tickers (ticker STRING, openvalues FLOAT, closeValue FLOAT,adjustedThe FLOAT,low FLOAT,high FLOAT,volume FLOAT,day DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "skip.header.line.count"="1"
)
STORED AS TEXTFILE
LOCATION  '/Users/teo/Downloads/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv';
LOAD DATA LOCAL INPATH '/Users/teo/Downloads/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv'
OVERWRITE INTO TABLE tickers;

CREATE TABLE IF NOT EXISTS sectors (ticker STRING,exc STRING, name STRING,sector STRING,industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "skip.header.line.count"="1"
)
STORED AS TEXTFILE
LOCATION  '/Users/teo/Downloads/daily-historical-stock-prices-1970-2018/historical_stocks.csv';
LOAD DATA LOCAL INPATH '/Users/teo/Downloads/daily-historical-stock-prices-1970-2018/historical_stocks.csv'
OVERWRITE INTO TABLE sectors;

DROP TABLE IF EXISTS sectors_values;
DROP TABLE IF EXISTS min_max_close;
DROP TABLE IF EXISTS xCent;


CREATE TABLE IF NOT EXISTS min_max_close AS
SELECT t.sector as sector, t.ticker_year as ticker_year, t.ticker as ticker, MIN(t.day) as min_day, MAX(t.day) AS max_day
FROM
(
    SELECT tickers.ticker as ticker, sectors.sector as sector, YEAR(tickers.day) as ticker_year, tickers.day AS day
    FROM tickers
    LEFT JOIN sectors ON tickers.ticker = sectors.ticker
    WHERE tickers.day >= '2008-01-01'
    GROUP BY sectors.sector, YEAR(tickers.day), tickers.ticker, tickers.day
) t
GROUP BY t.sector, t.ticker_year, t.ticker;

CREATE TABLE IF NOT EXISTS xCent AS
SELECT mx.sector as sector,mx.ticker_year as ticker_year,mx.ticker as ticker,(((maxs.maxClose-mins.minClose)/mins.minClose)*100) AS var
FROM min_max_close as mx, min_max_close as mnn
INNER JOIN
(
SELECT mn.sector as sector,mn.ticker_year as ticker_year,t.ticker as ticker,t.closeValue AS maxClose,mn.max_day as max_day
FROM tickers t
JOIN min_max_close mn ON t.day=mn.max_day AND t.ticker=mn.ticker
)maxs
ON maxs.ticker=mx.ticker AND maxs.max_day = mx.max_day AND maxs.sector=mx.sector
INNER JOIN
(
SELECT mn2.sector as sector,mn2.ticker_year as ticker_year,t.ticker as ticker, t.closeValue AS minClose,mn2.min_day as min_day
FROM tickers t
JOIN min_max_close mn2 ON t.day=mn2.min_day AND t.ticker=mn2.ticker
)mins
ON mnn.ticker=mins.ticker AND mnn.min_day= mins.min_day AND mins.sector=mnn.sector
WHERE mins.sector=maxs.sector AND mins.ticker=maxs.ticker AND mins.ticker_year=maxs.ticker_year;

CREATE TABLE IF NOT EXISTS sectors_values AS
SELECT v.sector AS sector,
       v.ticker_year as ticker_year,
       v.ticker AS ticker,
       SUM(t.volume) AS sumVolume,
       AVG(t.closeValue) AS avgQuote,
       v.var AS var
FROM tickers t
JOIN xCent v ON t.ticker = v.ticker
WHERE YEAR(t.day)=v.ticker_year
GROUP BY v.sector, v.ticker_year, v.ticker, v.var;

SELECT s.sector, s.ticker_year, AVG(s.var) as final_variation, AVG(s.avgQuote) AS final_quote, AVG(s.sumVolume) AS final_volume
FROM sectors_values s
GROUP BY s.sector, s.ticker_year
ORDER BY s.sector asc, s.ticker_year asc;