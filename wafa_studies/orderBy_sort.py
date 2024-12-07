from pyspark.sql import *
# orderBy and sort works same!
spark = SparkSession.builder.appName('with column').master('local[*]').getOrCreate()
df = spark.read.csv('data/employees.csv', header=True)
# df.sort('first_name').show()
# df.sort(df.first_name, df.last_name.desc()).show()
df.orderBy(df.first_name, df.last_name.desc()).show()
