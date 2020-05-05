#!/home/federico/anaconda3/bin/python
import sys
from operator import itemgetter

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


def printResults(ResultDict):
    for t in ResultDict.keys():
        if len(ResultDict[t]) > 1:
            print(
                ';'.join(map(str, ResultDict[t])) + ": " + '2016: ' + str(t[0][1]) + '% ,' + '2017: ' + str(
                    t[1][1]) + '% ,' + '2018: ' + str(t[2][1]) + '%')


def TakeColumn(Lists, year):
    ColumnList = []
    for List in Lists:
        if List[0] == year:
            ColumnList.append(List[1])
    return ColumnList


def avg(List):
    List = list(map(int, List))
    return str(round(sum(List) / len(List)))


def joinNames(ResultDict):
    for name in ResultDict.keys():
        if len(ResultDict[name]) > 1:
            column2016 = TakeColumn(ResultDict[name], '2016')
            column2017 = TakeColumn(ResultDict[name], '2017')
            column2018 = TakeColumn(ResultDict[name], '2018')
            if len(column2016) != 0 and len(column2017) != 0 and len(column2018) != 0:
                ResultDict[name] = ((
                    ('2016', str(avg(column2016))), ('2017', str(avg(column2017))),
                    ('2018', str(avg(column2018)))))
        else:
            ResultDict[name] = ResultDict[name][0]
    return ResultDict


def reducer():
    h = {}
    name = {}
    ListOfDays = []
    for line in sys.stdin.readlines():
        if len(line.strip().split("\t")) == 3:
            Ticker, CloseValue, Date = line.strip().split("\t")
            ListOfDays.append((Ticker, Date, CloseValue))
        else:
            Ticker, Name = line.strip().split("\t")
            name[Ticker] = h.get(Ticker, "") + Name
    ListOfDays = sorted(ListOfDays, key=itemgetter(1))
    for TickerByDay in ListOfDays:
        Ticker = TickerByDay[0]
        CloseValue = TickerByDay[2]
        Date = TickerByDay[1]
        h[Ticker, Date.split("-")[0]] = h.get((Ticker, Date.split("-")[0]), []) + [CloseValue]
    h2 = {}
    Final = groupByYearAndVariance(h)
    for x in Final:
        tickercompany = printName(x[0], name)
        h2[tickercompany] = h2.get(tickercompany, []) + [(x[1], x[2])]
    h2 = joinNames(h2)
    res = {}
    for key, val in sorted(h2.items()):
        if len(val) == 3:
            res[tuple(val)] = res.get(tuple(val), []) + [key]
    printResults(res)


if __name__ == '__main__':
    reducer()
