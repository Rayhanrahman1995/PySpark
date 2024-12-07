from pyspark.sql import *

spark = SparkSession.builder.appName('with column').master('local[*]').getOrCreate()
df = spark.read.csv('data/employees.csv', header=True)
# df.filter(df.gender == 'F').show()
df.where((df.gender == 'F') & (df.emp_no == 10002)).show()