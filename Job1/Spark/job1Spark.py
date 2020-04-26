import pyspark
import sys
from operator import add

args = str(sys.argv)
spark = pyspark.sql.SparkSession.builder.appName("Job1Spark").getOrCreate()
lines = spark.read.text(sys.argv[1]).rdd.map(lambda row : row[0]).take(100)

print(lines)
print("----------------------------------------------")
