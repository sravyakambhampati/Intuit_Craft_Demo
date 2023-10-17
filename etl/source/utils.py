from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, isnan, when, regexp_replace, count


def initialize_spark_session(app_name):
    """
    Initialize a Spark session.
    Args:
        app_name (str): The name of the Spark application.
    Returns:
        SparkSession: The initialized Spark session.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def rename_columns(input_df: DataFrame, columns_mapping) -> DataFrame:
    """
    Rename columns in a PySpark DataFrame based on a mapping.
    Args:
        input_df (DataFrame): The input DataFrame.
        columns_mapping (dict): A dictionary mapping old column names to new column names.
    Returns:
        DataFrame: The DataFrame with renamed columns.
    """
    for old_col, new_col in columns_mapping.items():
        input_df = input_df.withColumnRenamed(old_col, new_col)
    return input_df


def get_null_counts(input_df: DataFrame) -> DataFrame:
    """
    Get the count of null or NaN values in each column of a DataFrame.
    Args:
        input_df (DataFrame): The input DataFrame.
    :returns: Dataframe that contains count of null, nan or empty cells
    """
    return input_df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in input_df.columns])


def clean_data(dataframe: DataFrame, columns: list) -> DataFrame:
    """
    Cleans if any special characters exist in columns
    :param dataframe: Input data frame
    :param columns: Columns that need to be cleaned
    :return: returns the dataframe
    """
    # Iterate through the specified columns and apply data cleaning
    for colum in columns:
        # Replace single quotes and special characters with spaces
        dataframe = dataframe.withColumn(colum, regexp_replace(dataframe[colum], "[^a-zA-Z0-9\s]", ""))
    return dataframe
