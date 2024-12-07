from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('load csv').master('local[*]').getOrCreate()
df = spark.read.format('csv').option(key='header', value=True).load(path='data/employees.csv')
help(spark.read.json)