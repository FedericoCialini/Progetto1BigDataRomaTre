#!/home/federico/anaconda3/bin/python
import sys


def finalVariance(OpenValue, FinalValue):
    OpenValue = float(OpenValue)
    FinalValue = float(FinalValue)
    return round(((FinalValue - OpenValue) / OpenValue) * 100)


h = {}
for line in sys.stdin.readlines():
    Ticker, CloseValue, Year = line.strip().split("\t")
    h[Ticker, Year] = h.get((Ticker, Year), []) + [CloseValue]
h2 = {}
Visited = []
Final = []
for t in h.keys():
    if t not in Visited:
        finalVar = finalVariance(h[t][0], h[t][-1])
        Final.append((t[0], t[1], finalVar))
        Visited.append(t)
for x in Final:
    h2[x[0]] = h2.get(x[0], "") + str(x[1]) + ":" + str(x[2]) + "% "
res = {}
for key, val in sorted(h2.items()):
    if len(val.split(" ")) == 4:
        res[val] = res.get(val, []) + [key]
for t in res.keys():
    if len(res[t]) > 1:
        print(' '.join(map(str, res[t])) + ": " + t)
