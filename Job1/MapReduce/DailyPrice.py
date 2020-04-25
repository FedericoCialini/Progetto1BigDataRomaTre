#!/home/federico/anaconda3/bin/python
class DailyPrice:
    def __init__(self, ticker, openvalue, closevalue, lowthe, highthe, volume,date):
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
