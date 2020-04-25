import sys


class DailyPrice:
    def __init__(self, ticker, openvalue, closevalue, lowthe, highthe, volume, date):
        self.ticker = ticker
        self.openvalue = openvalue
        self.closevalue = closevalue
        self.lowthe = lowthe
        self.highthe = highthe
        self.volume = volume
        self.date = date

    def __eq__(self, other):
        return self.ticker == other.ticker and self.openvalue == other.openvalue and self.closevalue == other.closevalue and self.lowthe == other.lowthe and self.highthe == other.highthe

    def __hash__(self):
        return hash(self.ticker) + self.openvalue + self.closevalue + self.highthe + self.lowthe


h = {}
for line in sys.stdin.readlines():
    Ticker, OpenValue, CloseValue, LowThe, HighThe, volume, Date = line.strip().split("\t")
    price = DailyPrice(Ticker, OpenValue, CloseValue, LowThe, HighThe, volume, Date)
    h[Ticker] = h.get(Ticker, []) + [price]
finalList = []
for Ticker in h.keys():
    prices = h[Ticker]
    dates = []
    maxvalues = []
    minvalues = []
    volumes = []
    for price in prices:
        dates.append(price.date)
        volumes.append(float(price.volume))
        maxvalues.append(float(price.highthe))
        minvalues.append(float(price.lowthe))
    openValue = float(prices[0].closevalue)
    closeValue = float(prices[-1].closevalue)
    FinalVariance = round(((closeValue - openValue) / openValue) * 100)
    finalList.append((Ticker + "\t" + "\t" + str(FinalVariance) + "%" + "\t" + str(max(maxvalues)) +"\t" + str(min(minvalues)) + "\t" +str(sum(volumes)/len(volumes)),FinalVariance))
sortedList = sorted(finalList,key = lambda x: x[1],reverse=True)
for s in sortedList:
    print(s[0])

