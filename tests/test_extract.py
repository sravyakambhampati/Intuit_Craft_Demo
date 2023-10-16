import pytest
from etl.source.extract import extract_from_sources
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()


@pytest.mark.parametrize("file_format, options, expected_columns", [
    ("csv", {}, ["_c0", "_c1"]),
    ("csv", {"header": "true"}, ["reading_25_percentile", "reading_75_percentile"]),
    ("parquet", {"header": "true"}, ["reading_25_percentile", "reading_75_percentile"])
])
def test_extract_from_sources(spark_session, file_format, options, expected_columns):
    file_path = "tests_data/sample." + file_format

    df = extract_from_sources(spark_session, file_path, file_format, options)
    print(df.columns)

    assert df.columns == expected_columns
