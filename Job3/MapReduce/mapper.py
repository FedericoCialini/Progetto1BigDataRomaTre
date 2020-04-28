#!/home/federico/anaconda3/bin/python
import sys


def mapping():
    lines = sys.stdin.readlines()
    prices = lines[1:]
    for line in prices:
        if len(line.strip().split(",")) == 8:
            Ticker, OpenValue, CloseValue, Adj_close, LowThe, HighThe, Volume, Date = line.strip().split(",")
            year = Date.split("-")[0]
            if year >= '2016':
                print(Ticker, CloseValue, year, sep='\t')
        else:
            try:
                Ticker, Exchange, Name, Sector, Industry = line.strip().split(",")
                print(Ticker, Name, sep='\t')
            except:
                continue


if __name__ == '__main__':
    mapping()
