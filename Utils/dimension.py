import sys
newRows = []
rows = []
final=[]
lines = sys.stdin.readlines()
for line in lines[1:]:
    fields = line.strip().split(",")
    fields[0] = 'A' + fields[0] #inserisci B per il triplo,C per il quadruplo
    fields = ",".join(fields)
    rows.append(line)
    newRows.append(fields)
for line in Rows:#rimuovi per il triplo ed il quadruplo
    print(line)
for line in newRows: #newRows[0:int(len(newRows)/2)] per gli incrementi del 50%
    print(line)
