import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


def get_last_known_global_ranking(uni_glb_rnk: DataFrame) -> DataFrame:
    """
    Get the last known global ranking for each institution.

    Args:
        uni_glb_rnk (pyspark.sql.DataFrame): DataFrame containing global ranking data.

    Returns:
        pyspark.sql.DataFrame: DataFrame with the last known global ranking for each institution.
    """
    last_known_ranking = uni_glb_rnk.groupBy("institution").agg(F.max("year").alias("year"))
    last_known_global_ranking = last_known_ranking.join(uni_glb_rnk, on=["institution", "year"], how="inner")
    return last_known_global_ranking


def join_admissions_with_ranking(admissions_df: DataFrame, last_known_global_ranking: DataFrame) -> DataFrame:
    """
    Join admissions data with the last known global ranking data.

    Args:
        admissions_df (pyspark.sql.DataFrame): DataFrame containing admissions data.
        last_known_global_ranking (pyspark.sql.DataFrame): DataFrame containing the last known global ranking.

    Returns:
        pyspark.sql.DataFrame: DataFrame with admissions data joined with global ranking data.
    """
    return admissions_df.join(last_known_global_ranking, admissions_df.Name == last_known_global_ranking.institution,
                              "outer")


def filter_and_pivot_ranking(df: DataFrame) -> DataFrame:
    """
    Filter universities with a world rank less than or equal to max_rank and pivot the data by year.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame containing global ranking data.
        max_rank (int): Maximum world rank for filtering.
        years (list): List of years for pivoting.

    Returns:
        pyspark.sql.DataFrame: DataFrame with universities filtered by rank and pivoted by year.
    """
    pivoted_df = df.groupBy("institution").pivot("year").agg(F.first("world_rank"))
    return pivoted_df


def filter_top_n_ranked_universities(df: DataFrame, n) -> DataFrame:
    """
    Filter universities based on their ranking and select the top n universities.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame containing ranking data.
        n (int): Number of top universities to select.

    Returns:
        pyspark.sql.DataFrame: DataFrame with the top n ranked universities.
    """
    df.printSchema()
    window = Window.orderBy(["world_rank", F.desc("year")])
    top_n_universities = df.withColumn("new_world_ranking", F.row_number().over(window)).filter(F.col("new_world_ranking") <= n)
    return top_n_universities


def calculate_state_trends(admissions_df: DataFrame) -> DataFrame:
    """
    Calculate state-wise trends for the number of applicants and admissions.

    Args:
        admissions_df (pyspark.sql.DataFrame): Input DataFrame containing admissions data.

    Returns:
        pyspark.sql.DataFrame: DataFrame with state-wise trends.
    """
    state_trends = admissions_df.groupBy("state_abbreviation").agg(
        F.sum("applicants_total").alias("total_applicants"),
        F.sum("admissions_total").alias("total_admissions")
    )
    return state_trends


def calculate_state_trends_global(admissions_with_ranking: DataFrame) -> DataFrame:
    """
    Calculate state-wise trends for the number of applicants and admissions per year.

    Args:
        admissions_with_ranking (pyspark.sql.DataFrame): Input DataFrame containing admissions data with ranking.

    Returns:
        pyspark.sql.DataFrame: DataFrame with state-wise trends per year.
    """
    state_trends = admissions_with_ranking.groupBy("state_abbreviation", "year").agg(
        F.sum("applicants_total").alias("total_applicants"),
        F.sum("admissions_total").alias("total_admissions")
    )
    return state_trends
