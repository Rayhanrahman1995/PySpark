from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Create DataFrame') \
    .master('local[*]') \
    .config('driver-memory', '12G') \
    .getOrCreate()
print(spark)
# spark2 = spark.newSession()
# print(spark2)
# help(StructField)
data = [(1, 'Maher'), (2, 'Wafa')]
schema = StructType([StructField(name='id', dataType=IntegerType()),
                     StructField(name='name', dataType=StringType())])

df = spark.createDataFrame(data=data, schema=schema)
# df.show()
# df.printSchema()
