import sys

# import datetime
lines = sys.stdin.readlines()
prices = lines[1:]
for line in prices:
    Ticker, OpenValue, CloseValue, Adj_close, LowThe, HighThe, Volume, Date = line.strip().split(",")
    # date = datetime.datetime.strptime(Date,'%Y-%m-%d') //troppo lenta!
    year = Date.split("-")[0]
    if int(year) >= 2008:
        print(Ticker, OpenValue, CloseValue, LowThe, HighThe, Volume, Date, sep='\t')