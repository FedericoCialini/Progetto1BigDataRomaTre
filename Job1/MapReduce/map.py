import sys

lines = sys.stdin.readlines()
prices = lines[1:]
for line in prices:
    Ticker, OpenValue, CloseValue, Adj_close, LowThe, HighThe, Volume, Date = line.split(",")
    # DailyVariance = float(CloseValue) - float(OpenValue)
    print(Ticker, OpenValue, CloseValue, LowThe, HighThe, Volume, sep=',')
