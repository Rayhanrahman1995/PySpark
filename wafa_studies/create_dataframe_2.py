from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create df').master('local[*]').getOrCreate()
data = [{'id': 1, 'name': 'Maher'},
        {'id': 2, 'name': 'Wafa'}
        ]
df = spark.createDataFrame(data=data)
# df.show()
# print(df)
df.count()
