from pyspark.sql import SparkSession


def get_spark_object(env, appName):
    try:

        if env == 'QA':
            master = 'local'

        else:
            master = 'Yarn'

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
        return spark
    except Exception as exp:
        print(str(exp))
