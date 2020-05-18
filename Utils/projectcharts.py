import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# inserisci i dati locali
timesMrjob1 = [88, 125, 172, 215, 271, 301, 333]  
timesMrjob2 = [74, 121, 169, 213, 266, 305, 329]  
timesMrjob3 = [66, 97, 130, 160, 197, 228, 251]  
timesSpjob1 = [78, 124, 160, 219, 231, 285, 303]  
timesSpjob2 = [70, 117, 142, 189, 211, 255, 289]  
timesSpjob3 = [63, 85, 96, 137, 162, 187, 212]  
timesHvjob1 = [288, 334, 404, 471, 516, 579, 627]
timesHvjob2 = [275, 313, 353, 412, 457, 476, 514]  
timesHvjob3 = [267, 296, 320, 349, 381, 414, 454]  
# inserisci i dati aws
timesMrjob1aws = [41, 53, 78, 85, 104, 123, 140]  
timesMrjob2aws = [39, 44, 66, 77, 95, 101, 127]  
timesMrjob3aws = [33, 40, 56, 71, 82, 99, 114]  
timesSpjob1aws = [55, 80, 119, 137, 184, 210, 248]  
timesSpjob2aws = [57, 85, 127, 167, 196, 234, 265]  
timesSpjob3aws = [53, 79, 118, 134, 151, 176, 207]  
timesHvjob1aws = [240, 299, 349, 384, 423, 452, 491]  
timesHvjob2aws = [265, 304, 360, 383, 419, 446, 473]  
timesHvjob3aws = [260, 282, 314, 326, 331, 350, 378]  
# confronto numero nodi #locale,5,7,8 nodi per il quadruplo
nodesMrjob1 = [333, 140, 89, 71]
nodesHvjob1 = [627, 491, 380, 352]
nodesSpjob1 = [303, 248, 193, 157]
nodesMrjob2 = [329, 127, 73, 65]
nodesHvjob2 = [514, 473, 404,381]
nodesSpjob2 = [289, 265, 191, 159]
nodesMrjob3 = [251, 114, 62, 58]
nodesHvjob3 = [454, 378, 337, 311]
nodesSpjob3 = [212, 207, 180, 148]
dimension = ['20.97', '31.46', '41.95', '52.44', '62.92', '73.41',
             '83.90']  # normale,+50,+100,+150,+200,+250,+300%
numnodes=['locale','5','7','8',]
local=[(timesMrjob1,timesSpjob1,timesHvjob1,'cyan','deeppink','lime','Job1'),(timesMrjob2,timesSpjob2,timesHvjob2,'orangered','blueviolet','mediumspringgreen','Job2'),(timesMrjob3,timesSpjob3,timesHvjob3,'red','blue','yellow','Job3')]
aws=[(timesMrjob1aws,timesSpjob1aws,timesHvjob1aws,'cyan','deeppink','lime','Job1 aws'),(timesMrjob2aws,timesSpjob2aws,timesHvjob2aws,'orangered','blueviolet','mediumspringgreen','Job2 aws'),(timesMrjob3aws,timesSpjob3aws,timesHvjob3aws,'red','blue','yellow','job3 aws')]
nodes=[(nodesMrjob1,nodesSpjob1,nodesHvjob1,'cyan','deeppink','lime','Job1'),(nodesMrjob2,nodesSpjob2,nodesHvjob2,'orangered','blueviolet','mediumspringgreen','Job2'),(nodesMrjob3,nodesSpjob3,nodesHvjob3,'red','blue','yellow','Job3')]

for j in nodes:
    plt.plot(numnodes, j[0],linestyle='-',marker='o',markersize=8,color='orange', label='Tempo Map Reduce')
    plt.plot(numnodes, j[1],linestyle='-',marker='o',markersize=8,color='darkviolet', label='Tempo Spark')
    plt.plot(numnodes, j[2],linestyle='-',marker='o',markersize=8, color='black', label='Tempo Hive')
    plt.legend(bbox_to_anchor=(1, 1), loc='upper right', borderaxespad=0.01)
    plt.title(j[6])
    plt.xlabel("Numero di nodi")
    plt.ylabel("Tempo di esecuzione(secondi)")
    plt.grid()
    plt.show()

index = np.arange(7)
width = 0.15
for j in aws:
    plt.bar(index, j[0], width, color=j[3], label='Tempo Map Reduce')
    plt.bar(index + width, j[1], width, color=j[4], label='Tempo Spark')
    plt.bar(index + (width * 2), j[2], width, color=j[5], label='Tempo Hive')
    plt.title(j[6])
    plt.xlabel("Dimensione Input(milioni di righe)")
    plt.ylabel("Tempo di esecuzione(secondi)")
    plt.xticks(index + width / 2, dimension)
    plt.legend(loc='best')
    plt.show()
