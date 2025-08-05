import pyspark.sql
from pyspark.sql.types import StringType, StructField, DecimalType, FloatType, StructType
from pyspark.sql import Row
import pytest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts import CashForecastRefinedToProduced
from Utilities import utils


@pytest.fixture(scope="session")
def spark_session():
    print("Running CashForecastRefinedToProducedTests...")
    return utils.get_spark_session("CashForecastRefinedToProducedTests")


def test_create_temp_df(spark_session):
    print("Testing test_create_temp_df() method...")
    actual_df = CashForecastRefinedToProduced.create_temp_df(spark=spark_session)
    assert type(actual_df) == pyspark.sql.DataFrame
    actual_op = [row.asDict() for row in CashForecastRefinedToProduced.create_temp_df(spark=spark_session).limit(5).collect()]
    expected_op = [{'col1': 'test1', 'col2': 'test1'}, {'col1': 'test2', 'col2': 'test2'},
                   {'col1': 'test3', 'col2': 'test3'}, {'col1': 'test4', 'col2': 'test4'},
                   {'col1': 'test5', 'col2': 'test5'}]

    assert actual_op == expected_op


def test_add_col_a_and_b(spark_session):
    print("Testing test_add_col_a_and_b() method...")
    sample_data = [{'a': 5, 'b': 234},
                   {'a': 453, 'b': -543},
                   {'a': 22, 'b': 564},
                   {'a': 234, 'b': 1231},
                   {'a': 234, 'b': 348}]
    sample_df = spark_session.createDataFrame(sample_data)

    expected_op = [Row(a=5, b=234, c=239),
                   Row(a=453, b=-543, c=-90),
                   Row(a=22, b=564, c=586),
                   Row(a=234, b=1231, c=1465),
                   Row(a=234, b=348, c=582)]
    actual_df = CashForecastRefinedToProduced.add_col_a_and_b(sample_df)
    assert sorted(actual_df.collect()) == sorted(expected_op)


def test_calculate_rb_ar_ap_mixed_values(spark_session):
    """Test with mixed positive and negative values"""
    schema = StructType([
        StructField("deal_key", StringType(), True),
        StructField("due_date_reason", StringType(), True),
        StructField("total_amt_due", FloatType(), True)
    ])

    test_data = [
        ("DEAL001", "REASON1", 100.00),
        ("DEAL001", "REASON1", -50.00),  # Sum = 50.00 (positive -> 'R')
        ("DEAL002", "REASON2", -100.00),
        ("DEAL002", "REASON2", 50.00),  # Sum = -50.00 (negative -> 'P')
        ("DEAL003", "REASON3", -200.00),
        ("DEAL003", "REASON3", 200.00)  # Sum = 0.00 (zero -> 'R')
    ]

    df = spark_session.createDataFrame(test_data, schema)
    result_df = CashForecastRefinedToProduced.calculate_rb_ar_ap(df)
    results = result_df.collect()

    # Group results by deal_key
    deal001_results = [row for row in results if row["deal_key"] == "DEAL001"]
    deal002_results = [row for row in results if row["deal_key"] == "DEAL002"]
    deal003_results = [row for row in results if row["deal_key"] == "DEAL003"]

    # Assertions
    assert all(row["ar_ap"] == "R" for row in deal001_results)  # Sum = 50.00
    assert all(row["ar_ap"] == "P" for row in deal002_results)  # Sum = -50.00
    assert all(row["ar_ap"] == "R" for row in deal003_results)  # Sum = 0.00
