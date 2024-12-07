from pyspark.sql import *

# data = [(1, 'maher', 3000), (2, 'wafa', 4000)]
# schema = ['id', 'name', 'salary']
spark = SparkSession.builder.appName('create df').master('local[*]').getOrCreate()
# df = spark.createDataFrame(data=data, schema=schema)
# df.show()
row = Row(name='maher', salary=2000)
# print(row[0] + ' ' + str(row[1]))
# print(row.name + ' ' + str(row.salary))
print(row)
