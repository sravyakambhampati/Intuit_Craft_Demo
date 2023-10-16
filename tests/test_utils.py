import pytest
from pyspark.sql import SparkSession
from etl.source.utils import initialize_spark_session, rename_columns, get_null_counts, \
    get_colleges_apply_vs_high_probability, clean_data


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


# Test the get_colleges_apply_vs_high_probability function
def test_get_colleges_apply_vs_high_probability(spark_session):
    # Create a DataFrame for testing
    data = [(1, 500, 580, 540, 640, 700, 630), (2, 420, 420, 325, 500, 530, 510), (3, 400, 550, 600, 485, 500, 545)]
    columns = ["ID", "reading_25_percentile", "math_25_percentile", "writing_25_percentile", "reading_75_percentile",
               "math_75_percentile", "writing_75_percentile"]
    df = spark_session.createDataFrame(data, columns)
    applicant_scores = {"sat_reading_score": 500, "sat_math_score": 650, "sat_writing_score": 550}
    recommended, high_probability = get_colleges_apply_vs_high_probability(applicant_scores, df)
    assert len(recommended.head(10)) > 0
    assert len(high_probability.head(10)) > 0


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
