import matplotlib.pyplot as plt
import numpy as np

# inserisci i dati
timesMrjob1 = [1, 2, 3, 4, 5]
timesMrjob2 = [2, 2, 4, 5, 5]
timesMrjob3 = [3, 4, 8, 5, 5]
timesSpjob1 = [1, 2, 3, 4, 5]
timesSpjob2 = [2, 2, 4, 5, 5]
timesSpjob3 = [3, 4, 8, 5, 5]
timesHvjob1 = [1, 2, 3, 4, 5]
timesHvjob2 = [2, 2, 4, 5, 5]
timesHvjob3 = [3, 4, 8, 5, 5]
dimension = [20973890, 26217361, 31460833, 36704305, 41947778] # normale,+25,+50,+75,+100%
# --------------------
plt.plot(dimension, timesMrjob1, color='blue')
plt.plot(dimension, timesMrjob2, color='red')
plt.plot(dimension, timesMrjob3, color='green')
plt.title("Big Data charts")
plt.xlabel("Dimensione Input")
plt.ylabel("Tempo di esecuzione")
plt.show()
dimension = list(map(str, dimension))
index = np.arange(3)
width = 0.25
plt.bar(index, timesMrjob1, width, color='blue', label='Tempo Job1')
plt.bar(index + width, timesMrjob2, width, color='red', label='Tempo Job2')
plt.bar(index + (width * 2), timesMrjob3, width, color='green', label='Tempo Job3')
plt.title("Big Data charts")
plt.xlabel("Dimensione Input")
plt.ylabel("Tempo di esecuzione")
plt.xticks(index + width / 2, dimension)
plt.legend(loc='best')
plt.show()
