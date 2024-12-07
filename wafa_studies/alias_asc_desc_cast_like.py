from pyspark.sql import *

spark = SparkSession.builder.appName('alias_asc_desc_cast_like').master('local[*]').getOrCreate()
df = spark.read.csv(path="data/employees.csv", header=True)
# df_2 = df.select(df.emp_no, df.first_name, df.gender.alias('sex'))
# df_2 = df.filter(df.first_name.like('M%'))
df_2 = df.select("*").filter(df.first_name.like('M%'))
df_2.show()
