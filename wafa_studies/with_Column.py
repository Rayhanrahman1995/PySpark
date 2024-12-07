from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName('with column').master('local[*]').getOrCreate()
df = spark.read.csv('data/employees.csv', header=True)
# df1 = df.select(col('id').alias('employee_id'))
# df.show(n=2, truncate=False, vertical=True)
# df.show()
# df.printSchema()
# To change column 'salary' data-type string to integer
# df2 = df.withColumn(colName='salary', col=col('salary').cast('Integer'))
# df2.show()
df2 = df.select(df.emp_no, df.first_name, df.gender,
                when(df.gender == 'M', 'male').when(df.gender == 'F', 'female').otherwise('unknown').alias('gender_1'))
df2.show()
