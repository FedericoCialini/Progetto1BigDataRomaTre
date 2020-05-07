file = '/home/federico/PycharmProjects/progetto1BigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv'
newRows = []
rows = []
with open(file, 'r') as f:
    lines = f.readlines()
    for line in lines[1:]:
        fields = line.strip().split(",")
        fields[0] = 'FAKE' + fields[0]
        fields = ",".join(fields)
        rows.append(line)
        newRows.append(fields)
with open(
        '/home/federico/PycharmProjects/progetto1BigData/daily-historical-stock-prices-1970-2018/new_historical_stock_prices_double.csv',
        'a') as f2:
    for line in rows:
        f2.write(line)
    for line in newRows:
        f2.write(line + '\n')

