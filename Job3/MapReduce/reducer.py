#!/home/federico/anaconda3/bin/python
import sys


def finalVariance(OpenValue, FinalValue):
    OpenValue = float(OpenValue)
    FinalValue = float(FinalValue)
    return round(((FinalValue - OpenValue) / OpenValue) * 100)


def printName(x, name):
    try:
        return name[x]
    except:
        return x


def groupByYearAndVariance(TickerDict):
    Visited = []
    Final = []
    for t in TickerDict.keys():
        if t not in Visited:
            finalVar = finalVariance(TickerDict[t][0], TickerDict[t][-1])
            Final.append((t[0], t[1], finalVar))
            Visited.append(t)
    return Final


def printResults(ResultDict, NameDict):
    for t in ResultDict.keys():
        if len(ResultDict[t]) > 1:
            print(','.join(map(str, map(lambda x: printName(x, NameDict), ResultDict[t]))) + ": " + t)


def reducer():
    h = {}
    name = {}
    for line in sys.stdin.readlines():
        if len(line.strip().split("\t")) == 3:
            Ticker, CloseValue, Year = line.strip().split("\t")
            h[Ticker, Year] = h.get((Ticker, Year), []) + [CloseValue]
        else:
            Ticker, Name = line.strip().split("\t")
            name[Ticker] = h.get(Ticker, "") + Name
    h2 = {}
    Final = groupByYearAndVariance(h)
    for x in Final:
        h2[x[0]] = h2.get(x[0], "") + str(x[1]) + ":" + str(x[2]) + "% "
    res = {}
    for key, val in sorted(h2.items()):
        if len(val.split(" ")) == 4:
            res[val] = res.get(val, []) + [key]
    printResults(res, name)


if __name__ == '__main__':
    reducer()
