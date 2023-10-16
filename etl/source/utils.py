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


def get_colleges_apply_vs_high_probability(applicant_scores, df):
    """
    Filter colleges based on applicant's SAT scores and return colleges to apply and high probability colleges.
    Args:
        applicant_scores (dict): Dictionary containing SAT scores.
        df (DataFrame): DataFrame containing SAT scores.
    Returns:
        DataFrame: Recommended colleges to apply.
        DataFrame: High probability colleges.
    """
    filtered_colleges = df.withColumn("colleges_recommended", when(
        (col("reading_25_percentile") <= applicant_scores["sat_reading_score"]) &
        (col("math_25_percentile") <= applicant_scores["sat_math_score"]) &
        (col("writing_25_percentile") <= applicant_scores["sat_writing_score"]), 1).otherwise(0))

    recommended = filtered_colleges.filter(col("colleges_recommended") == 1).orderBy(desc("reading_25_percentile"),desc("writing_25_percentile"),desc("math_25_percentile"))

    filtered_colleges = filtered_colleges.withColumn("high_prob_colleges", when(
        (col("reading_75_percentile") <= applicant_scores["sat_reading_score"]) &
        (col("math_75_percentile") <= applicant_scores["sat_math_score"]) &
        (col("writing_75_percentile") <= applicant_scores["sat_writing_score"]), 1).otherwise(0))

    high_probability = filtered_colleges.filter(col("high_prob_colleges") == 1).orderBy(desc("reading_75_percentile"),desc("writing_75_percentile"),desc("math_75_percentile"))

    return recommended, high_probability

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
