

def get_current_date(spark):
    try:
        output = spark.sql("""select current_date""")
        print("validating spark object with current_date- " + str(output.collect()))

    except Exception as e:
        print(e)
