from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('with column').master('local[*]').getOrCreate()
df1 = spark.read.csv('data/emp_2.csv', header=True)
df2 = spark.read.csv('data/emp_3.csv', header=True)

df1.unionByName(df2, allowMissingColumns=True).show()

