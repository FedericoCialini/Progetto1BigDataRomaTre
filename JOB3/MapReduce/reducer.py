#!/home/federico/anaconda3/bin/python
import sys


def finalVariance(OpenValue, FinalValue):
    OpenValue = float(OpenValue)
    FinalValue = float(FinalValue)
    return round(((FinalValue - OpenValue) / OpenValue) * 100)


h = {}
for line in sys.stdin.readlines():
    Ticker, CloseValue, Date = line.strip().split("\t")
    h[Ticker, Date] = h.get((Ticker,Date),[]) + [CloseValue]
h2 = {}
for t in h.keys():
    if not t  in h2.keys():
        h2[t] = h2.get(t, 0) + finalVariance(h[t][0], h[t][-1])
