import pytest
from etl.source.load import save_as
from pyspark.sql import SparkSession
import os


@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("test").getOrCreate()


def test_save_as_parquet(spark_session, tmpdir):
    # Create a test DataFrame
    data = spark_session.createDataFrame([(1, 'Alice'), (2, 'Bob')], ["id", "name"])
    output_path = os.path.join(tmpdir, "test_parquet")

    # Call the function to save as Parquet
    save_as(data, output_path, "parquet")

    # Check if the Parquet file exists
    assert os.path.exists(output_path)


def test_save_as_orc(spark_session, tmpdir):
    # Create a test DataFrame
    data = spark_session.createDataFrame([(1, 'Alice'), (2, 'Bob')], ["id", "name"])
    output_path = os.path.join(tmpdir, "test_orc")

    # Call the function to save as ORC
    save_as(data, output_path, "orc")

    # Check if the ORC file exists
    assert os.path.exists(output_path)
