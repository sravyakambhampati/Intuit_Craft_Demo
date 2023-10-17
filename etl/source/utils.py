from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, regexp_replace, desc, count


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


def rename_columns(input_df, columns_mapping):
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


def get_null_counts(input_df):
    """
    Get the count of null or NaN values in each column of a DataFrame.
    Args:
        df (DataFrame): The input DataFrame.
    """
    return input_df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in input_df.columns])


def clean_data(dataframe, columns):
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
