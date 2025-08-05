from pyspark.sql import SparkSession


def get_spark_session(app_name):
    spark = SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark Session created with appName : {app_name}")
    print(f"Running on spark version {spark.version}")
    return spark
