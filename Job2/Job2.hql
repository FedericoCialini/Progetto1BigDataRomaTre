set hive.execution.engine = mr;


DROP TABLE IF EXISTS tickers;
DROP TABLE IF EXISTS sectors;
DROP TABLE IF EXISTS sectorYears;
DROP TABLE IF EXISTS sectors_values;
DROP TABLE IF EXISTS min_max_close;
DROP TABLE IF EXISTS xCent;
DROP TABLE IF EXISTS maxs;
DROP TABLE IF EXISTS mins;


CREATE TABLE IF NOT EXISTS tickers (ticker STRING, openvalues FLOAT, closeValue FLOAT,adjustedThe FLOAT,low FLOAT,high FLOAT,volume FLOAT,tickday DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "skip.header.line.count"="1"
)
STORED AS TEXTFILE
LOCATION  '/output';
LOAD DATA  INPATH '/input/historical_stock_prices.csv'
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
LOCATION  '/output';
LOAD DATA  INPATH '/input/historical_stocks.csv'
OVERWRITE INTO TABLE sectors;

CREATE TABLE IF NOT EXISTS sectorYears AS
SELECT tickers.ticker as ticker, sectors.sector as sector, YEAR(tickers.day) as ticker_year, tickers.tickday AS day,tickers.closeValue as closeValue,tickers.volume as volume
FROM sectors
JOIN tickers ON tickers.ticker = sectors.ticker
WHERE year(tickers.tickday) >= '2008';
    
CREATE TABLE IF NOT EXISTS min_max_close AS
SELECT sector , ticker_year, ticker, MIN(tickday) as min_day, MAX(tickday) AS max_day
FROM sectorYears
GROUP BY sector, ticker_year, ticker;

CREATE TABLE maxs AS
SELECT mn.sector as sector,mn.ticker_year as ticker_year,t.ticker as ticker,t.closeValue AS maxClose,mn.max_day as max_day
FROM  min_max_close mn
JOIN sectorYears t ON t.day=mn.max_day AND t.ticker=mn.ticker;

CREATE TABLE mins AS
SELECT mn2.sector as sector,mn2.ticker_year as ticker_year,t.ticker as ticker, t.closeValue AS minClose,mn2.min_day as min_day
FROM min_max_close mn2
JOIN sectorYears t ON t.day=mn2.min_day AND t.ticker=mn2.ticker;

CREATE TABLE IF NOT EXISTS xCent AS
SELECT mx.sector as sector,mx.ticker_year as ticker_year,mx.ticker as ticker,(((maxs.maxClose-mins.minClose)/mins.minClose)*100) AS var
FROM min_max_close as mx, min_max_close as mnn
JOIN maxs
ON maxs.ticker=mx.ticker AND maxs.max_day = mx.max_day
JOIN mins
ON mnn.ticker=mins.ticker AND mnn.min_day= mins.min_day 
WHERE mins.sector=maxs.sector AND mins.ticker=maxs.ticker AND mins.ticker_year=maxs.ticker_year;

CREATE TABLE IF NOT EXISTS sectors_values AS
SELECT v.sector AS sector,
       v.ticker_year as ticker_year,
       v.ticker AS ticker,
       SUM(t.volume) AS sumVolume,
       AVG(t.closeValue) AS avgQuote,
       v.var AS var
FROM xCent v JOIN sectorYears t ON t.ticker = v.ticker
WHERE YEAR(t.day)=v.ticker_year
GROUP BY v.sector, v.ticker_year,v.ticker,v.var;

SELECT s.sector, s.ticker_year, AVG(s.var) as final_variation, AVG(s.avgQuote) AS final_quote, AVG(s.sumVolume) AS final_volume
FROM sectors_values s
GROUP BY s.sector, s.ticker_year
ORDER BY s.sector asc, s.ticker_year asc;
