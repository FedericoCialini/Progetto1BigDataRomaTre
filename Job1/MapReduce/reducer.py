#!/home/federico/anaconda3/bin/python
import sys
from DailyPrice import DailyPrice


def reducer():
    TickerDictionary = createDictionary()
    sortedTickersByVariance = sortTickers(TickerDictionary)
    for s in sortedTickersByVariance:
        print(s[0])


def sortTickers(TickerDictionary):
    finalList = []
    for Ticker in TickerDictionary.keys():
        prices = TickerDictionary[Ticker]
        MaxValues = []
        MinValues = []
        volumes = []
        for price in prices:
            volumes.append(float(price.volume))
            MaxValues.append(float(price.highthe))
            MinValues.append(float(price.lowthe))
        prices = sorted(prices, key=lambda x: x.date, reverse=False)
        openValue = float(prices[0].closevalue)
        closeValue = float(prices[-1].closevalue)
        FinalVariance = round(((closeValue - openValue) / openValue) * 100)
        finalList.append((Ticker + "\t" + "\t" + str(FinalVariance) + "%" + "\t" + str(min(MinValues)) + "\t" +
                          str(max(MaxValues)) + "\t" + str(sum(volumes) / len(volumes)), FinalVariance))
    sortedTickerByVariance = sorted(finalList, key=lambda x: x[1], reverse=True)
    return sortedTickerByVariance


def createDictionary():
    TickerDict = {}
    for line in sys.stdin.readlines():
        Ticker, OpenValue, CloseValue, LowThe, HighThe, volume, date = line.strip().split("\t")
        price = DailyPrice(Ticker, OpenValue, CloseValue, LowThe, HighThe, volume, date)
        TickerDict[Ticker] = TickerDict.get(Ticker, []) + [price]
    return TickerDict


if __name__ == '__main__':
    reducer()
