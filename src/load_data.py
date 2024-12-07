from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName('loading csv') \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/employees.csv")

df.show()
df.printSchema()

column_1 = df.emp_no
column_2 = df['emp_no']
column_3 = col('emp_no')

df.select(column_1, column_2, column_3).show()
df.select('emp_no','birth_date','first_name').show()

date_as_string = df['birth_date'].cast(StringType()).alias("born")
df.select(df['birth_date'], date_as_string) \
      .show()

