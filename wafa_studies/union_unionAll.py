from pyspark.sql import *

spark = SparkSession.builder.appName('with column').master('local[*]').getOrCreate()
df1 = spark.read.csv('data/emp_1.csv', header=True)
df2 = spark.read.csv('data/emp_2.csv', header=True)
new_df = df1.unionAll(df2)
new_df.distinct().show()
