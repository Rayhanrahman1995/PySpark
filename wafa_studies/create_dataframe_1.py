from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Create DataFrame').master('local[*]').getOrCreate()
# help(StructField)
data = [(1, 'Maher'), (2, 'Wafa')]
schema = StructType([StructField(name='id', dataType=IntegerType()),
                     StructField(name='name', dataType=StringType())])

df = spark.createDataFrame(data=data, schema=schema)
# df.show()
df.printSchema()
