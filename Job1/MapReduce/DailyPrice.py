#!/home/federico/anaconda3/bin/python
class DailyPrice:
    def __init__(self, ticker, closevalue, volume, date):
        self.ticker = ticker
        self.closevalue = closevalue
        self.volume = volume
        self.date = date

    def __eq__(self, other):
        return self.ticker == other.ticker and self.closevalue == other.closevalue

    def __hash__(self):
        return hash(self.ticker) + self.closevalue
