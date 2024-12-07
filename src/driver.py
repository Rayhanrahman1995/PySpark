import get_env_variables as gav
from create_spark import get_spark_object
from validation import get_current_date


def main():
    print('I am in the main')
    print(gav.header)
    print(gav.env)

    spark = get_spark_object(gav.env, gav.appName)

    print('object created...', spark)

    get_current_date(spark)


if __name__ == '__main__':
    main()