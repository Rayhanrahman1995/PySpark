from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('reading csv file').master('local[*]').getOrCreate()
df = spark.read.csv(path="data/employees.csv", header=True)
# df.show()
# df.printSchema()
# help(spark)
df_2 = df.select("emp_no", "first_name").filter(col("emp_no") == 10001)
df_2.show()
# df_2.createTempView("data")
# spark.sql("select * from data").show()
