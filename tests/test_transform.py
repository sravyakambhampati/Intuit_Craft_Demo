import pytest
from etl.source.transform import get_last_known_global_ranking, filter_and_pivot_ranking, get_colleges_apply_vs_high_probability
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()


def test_get_last_known_global_ranking(spark_session):
    # Create a test DataFrame
    test_data = [("University A", 2022, 10), ("University B", 2022, 20), ("University A", 2023, 20)]
    columns = ["institution", "year", "world_rank"]
    df = spark_session.createDataFrame(test_data, columns)

    result_df = get_last_known_global_ranking(df)

    assert result_df.count() == 2  # Ensure the expected number of rows
    assert "institution" in result_df.columns
    assert "year" in result_df.columns
    assert "world_rank" in result_df.columns
    assert result_df.filter((result_df["institution"] == "University A") & (result_df["world_rank"] == 10)).count() == 0
    assert result_df.filter((result_df["institution"] == "University A") & (result_df["world_rank"] == 20)).count() == 1


def test_filter_and_pivot_ranking(spark_session):
    # Create a test DataFrame for input
    data = [(1, "University A", 2020, 5), (2, "University B", 2020, 15), (3, "University A", 2021, 8)]
    columns = ["id", "institution", "year", "world_rank"]
    input_df = spark_session.createDataFrame(data, columns)

    # Call the function with the test DataFrame
    result_df = filter_and_pivot_ranking(input_df)

    # Define the expected DataFrame
    expected_data = [("University A", 5, 8), ("University B", 15, None)]
    expected_columns = ["institution", "2020", "2021"]
    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    # Assert the two DataFrames are equal
    assert result_df.collect() == expected_df.collect()

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
