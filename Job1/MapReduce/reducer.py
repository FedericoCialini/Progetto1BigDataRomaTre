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
        maxvalues = []
        minvalues = []
        volumes = []
        for price in prices:
            volumes.append(float(price.volume))
            maxvalues.append(float(price.highthe))
            minvalues.append(float(price.lowthe))
        openValue = float(prices[0].closevalue)
        closeValue = float(prices[-1].closevalue)
        FinalVariance = round(((closeValue - openValue) / openValue) * 100)
        finalList.append((Ticker + "\t" + "\t" + str(FinalVariance) + "%" + "\t" + str(min(minvalues)) + "\t" +
                          str(max(maxvalues)) + "\t" + str(sum(volumes) / len(volumes)), FinalVariance))
    sortedTickerByVariance = sorted(finalList, key=lambda x: x[1], reverse=True)
    return sortedTickerByVariance


def createDictionary():
    TickerDict = {}
    for line in sys.stdin.readlines():
        Ticker, OpenValue, CloseValue, LowThe, HighThe, volume = line.strip().split("\t")
        price = DailyPrice(Ticker, OpenValue, CloseValue, LowThe, HighThe, volume)
        TickerDict[Ticker] = TickerDict.get(Ticker, []) + [price]
    return TickerDict


if __name__ == '__main__':
    reducer()
