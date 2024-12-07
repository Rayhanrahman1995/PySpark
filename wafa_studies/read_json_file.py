from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('read json').master('local[*]').getOrCreate()
# read single json file with multiline
# df = spark.read.json(path="data/emps.json", multiLine=True)
# df.printSchema()
# read multiple json file with multiline
# df = spark.read.json(path=["data/emps.json","data_1/emps_1.json"], multiLine=True)
# df.show()
# read multiple json file with multiline in a folder
schema = StructType(). \
    add(field='ceo',data_type=StringType()). \
    add(field='name',data_type=StringType()). \
    add(field='numberOfEmployees',data_type=IntegerType()). \
    add(field='rating',data_type=DoubleType())
df = spark.read.json(path=["data/companies.json"], multiLine=True,schema=schema)
df.show()
df.printSchema()

