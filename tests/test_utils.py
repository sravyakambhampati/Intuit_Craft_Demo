import pytest
from pyspark.sql import SparkSession
from etl.source.utils import rename_columns, get_null_counts, clean_data


# Initialize a Spark session for testing
@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()


# Test the initialize_spark_session function
def test_initialize_spark_session(spark_session):
    assert spark_session is not None


# Test the rename_columns function
def test_rename_columns(spark_session):
    data = [(1, "John", "New York"), (2, "Alice", "California")]
    columns = ["ID", "Name", "Location"]
    df = spark_session.createDataFrame(data, columns)
    columns_mapping = {"ID": "StudentID", "Location": "City"}
    renamed_df = rename_columns(df, columns_mapping)
    assert "StudentID" in renamed_df.columns
    assert "City" in renamed_df.columns


# Test the get_null_counts function
def test_get_null_counts(spark_session):
    data = [(1, "John"), (2, None), (3, "Alice")]
    columns = ["ID", "Name"]
    df = spark_session.createDataFrame(data, columns)
    null_counts = get_null_counts(df)
    assert null_counts.select("ID").first()[0] == 0
    assert null_counts.select("Name").first()[0] == 1


# Test the clean_data function
def test_clean_data(spark_session):
    data = [(1, "John", "New York"), (2, "Alice", "San Francisco"), (3, "Alice", "Au@stin")]
    columns = ["ID", "Name", "Location"]
    df = spark_session.createDataFrame(data, columns)
    columns_to_clean = ["Name", "Location"]
    cleaned_df = clean_data(df, columns_to_clean)
    assert cleaned_df.filter(cleaned_df["Name"] == "John").count() == 1
    assert cleaned_df.filter(cleaned_df["Location"] == "San Francisco").count() == 1
    assert cleaned_df.filter(cleaned_df["Location"] == "Au@stin").count() == 0
