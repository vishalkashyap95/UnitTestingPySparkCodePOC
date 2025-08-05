from Utilities import utils
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructType, StructField


def create_temp_df(spark):
    df_data = [{"col1": "test1", "col2": "test1"},
               {"col1": "test2", "col2": "test2"},
               {"col1": "test3", "col2": "test3"},
               {"col1": "test4", "col2": "test4"},
               {"col1": "test5", "col2": "test5"},
               {"col1": "test6", "col2": "test6"},
               {"col1": "test7", "col2": "test7"},
               {"col1": "test8", "col2": "test8"},
               {"col1": "test9", "col2": "test9"}
               ]
    df = spark.createDataFrame(df_data)
    return df


def add_col_a_and_b(df):
    return df.withColumn("c", F.col("a") + F.col("b"))


def calculate_rb_ar_ap(df):
    # Define a window specification to partition by the required columns
    # window_spec = Window.partitionBy("deal_key","cflow_type")due_date_reason
    window_spec = Window.partitionBy("deal_key", "due_date_reason")
    # Summing 'total_amt_due' by the given columns without reducing the row count
    df = df.withColumn("sum_total_amt_due",F.sum("total_amt_due").over(window_spec))

    # Adding 'ar_ap' column based on the provided logic
    df = df.withColumn("ar_ap",
                       F.when((df["sum_total_amt_due"] < 0), F.lit("P"))
                       .otherwise(F.lit("R")).cast(StringType()))

    # Remove the intermediate column
    df = df.drop("sum_total_amt_due")

    return df


if __name__ == "__main__":
    spark = utils.get_spark_session("CashForecastRefinedToProduced")
    print("Running Job : CashForecastRefinedToProduced")
    # df = create_temp_df(spark=spark)
    # df.show()
    # print([row.asDict() for row in df.collect()])
