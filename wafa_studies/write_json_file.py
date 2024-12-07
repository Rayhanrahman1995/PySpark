from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('write json file').master('local[*]').getOrCreate()
data = [{1, 'maher'}, {2, 'wafa'}]
schema = ['id', 'name']
df = spark.createDataFrame(data=data, schema=schema)
df.write.json('data/write.json')
