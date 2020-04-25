import sys


class DailyPrice:
    def __init__(self, ticker, openvalue, closevalue, lowthe, highthe, volume):
        self.ticker = ticker
        self.openvalue = openvalue
        self.closevalue = closevalue
        self.lowthe = lowthe
        self.highthe = highthe
        self.volume = volume
        #self.date = date

    def __eq__(self, other):
        return self.ticker == other.ticker and self.openvalue == other.openvalue and self.closevalue == other.closevalue and self.lowthe == other.lowthe and self.highthe == other.highthe

    def __hash__(self):
        return hash(self.ticker) + self.openvalue + self.closevalue + self.highthe + self.lowthe


h = {}
for line in sys.stdin.readlines():
    Ticker, OpenValue, CloseValue, LowThe, HighThe, volume = line.split(",")
    price = DailyPrice(Ticker, OpenValue, CloseValue, LowThe, HighThe, volume)
    h[Ticker] = h.get(Ticker, []) + [price]
    # print("a")
for Ticker in h.keys():
    prices = h[Ticker]
    openValue = float(prices[0].openvalue)
    closeValue = float(prices[-1].closevalue)
    FinalVariance = str(((closeValue - openValue) / openValue) * 100) + "%"
    maxvalues = []
    minvalues = []
    volumes = []
    # print("b")
    for price in prices:
        volumes.append(float(price.volume))
        maxvalues.append(float(price.highthe))
        minvalues.append(float(price.lowthe))
    print(Ticker, FinalVariance , max(maxvalues), min(minvalues),
          sum(volumes) / len(volumes), sep=" ")
