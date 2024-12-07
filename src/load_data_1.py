from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName('loading csv') \
    .master("local[*]") \
    .getOrCreate()


def load_stock_data(symbol: str):
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/" + symbol + ".csv")

    return df.select(
        df['emp_no'].cast(IntegerType()).alias("Emp_No"),
        df['birth_date'].cast(StringType()).alias("Birth_Date"),
        df['first_name'].cast(StringType()).cast("First_Name"),
        df['last_name'].cast(StringType()).cast("Last_Name"),
        df['gender'].cast(StringType()).cast("Gender"),
        df['hire_date'].cast(StringType()).cast("Hire_Date")
    )
    employees = load_stock_data("employees")
    employees.show()

    employees['']