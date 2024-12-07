from pyspark.sql import *

spark = SparkSession.builder.appName('with column').master('local[*]').getOrCreate()
df = spark.read.csv('data/employees.csv', header=True)

df.distinct().show()