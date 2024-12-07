from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('with column').master('local[*]').getOrCreate()
# df1 = spark.read.csv('data/emp_2.csv', header=True)
# df1.show()
# # df1.select()
