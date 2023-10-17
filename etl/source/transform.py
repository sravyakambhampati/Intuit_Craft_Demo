import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, desc


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
    top_n_universities = df.withColumn("new_world_ranking", F.row_number().over(window)).filter(
        F.col("new_world_ranking") <= n)
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


def calculate_state_trends_yearly(college_data: DataFrame, college_adm: DataFrame,
                                  college_adm_selected_apps: DataFrame) -> DataFrame:
    """
    Calculate state-wise trends for the number of applicants and admissions per year.
    First, add a column with state abbreviation. Union college data and admissions data assuming college data is 2016 and college data is from 2015

    Args:
        college_data (pyspark.sql.DataFrame): Input DataFrame containing college_data.
        college_adm : Input DataFrame containing college admissions

    Returns:
        pyspark.sql.DataFrame: DataFrame with state-wise trends per year.
    """
    college_data_2016 = college_data.join(college_adm, on=["Name"], how="inner").withColumn("year", F.lit(2016))
    admissions_data_2015 = college_adm_selected_apps.withColumn("year", F.lit(2015))
    college_data_2015_2016 = admissions_data_2015.union(college_data_2016)
    state_trends = college_data_2015_2016.dropna().groupBy("state_abbreviation", "year").agg(
        F.sum("applicants_total").alias("total_applicants"),
        F.sum("admissions_total").alias("total_admissions")
    )
    return state_trends


def get_colleges_apply_vs_high_probability(applicant_scores, df) -> DataFrame:
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

    recommended = filtered_colleges.filter(col("colleges_recommended") == 1).orderBy(desc("reading_25_percentile"),
                                                                                     desc("writing_25_percentile"),
                                                                                     desc("math_25_percentile"))

    filtered_colleges = filtered_colleges.withColumn("high_prob_colleges", when(
        (col("reading_75_percentile") <= applicant_scores["sat_reading_score"]) &
        (col("math_75_percentile") <= applicant_scores["sat_math_score"]) &
        (col("writing_75_percentile") <= applicant_scores["sat_writing_score"]), 1).otherwise(0))

    high_probability = filtered_colleges.filter(col("high_prob_colleges") == 1).orderBy(desc("reading_75_percentile"),
                                                                                        desc("writing_75_percentile"),
                                                                                        desc("math_75_percentile"))

    return recommended, high_probability
