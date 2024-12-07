from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('group by').master('local[*]').getOrCreate()
df = spark.read.csv('data/resource.csv', header=True)
df_1 = df.groupBy('dep').agg(min('salary').alias('min_salary'))
df_1.show()
# df.show()
