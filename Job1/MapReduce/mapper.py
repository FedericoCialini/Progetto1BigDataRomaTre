#!/home/federico/anaconda3/bin/python
import sys


def mapping():
    lines = sys.stdin.readlines()
    prices = lines[1:]
    for line in prices:
        Ticker, OpenValue, CloseValue, Adj_close, LowThe, HighThe, Volume, Date = line.strip().split(",")
        year = Date.split("-")[0]
        if year >= '2008':
            print(Ticker, CloseValue, Volume, Date, sep='\t')


if __name__ == '__main__':
    mapping()
