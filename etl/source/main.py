from extract import extract_from_sources
from utils import initialize_spark_session, rename_columns, get_colleges_apply_vs_high_probability, clean_data
from transform import get_last_known_global_ranking, join_admissions_with_ranking, filter_and_pivot_ranking, \
    filter_top_n_ranked_universities, calculate_state_trends, calculate_state_trends_global
from load import save_as

if __name__ == '__main__':
    spark = initialize_spark_session("CollegeAdmissionsETL")
    options = {"header": "true", "inferSchema": "true"}
    college_data = extract_from_sources(spark, "etl/data/College_Data.csv", "csv", options)
    uni_glb_rnk = extract_from_sources(spark, "etl/data/Universities_Global_Ranking.csv", "csv", options)
    college_adm = extract_from_sources(spark, "etl/data/College_Admissions.csv", "csv", options)
    uni_data = extract_from_sources(spark, "etl/data/University_Data.csv", "csv", options)

    columns_mapping = {
        "Applicants total": "Applicants_total",
        "Admissions total": "Admissions_total",
        "Enrolled total": "Enrolled_total",
        "State abbreviation": "State_abbreviation",
        "SAT Critical Reading 25th percentile score": "reading_25_percentile",
        "SAT Critical Reading 75th percentile score": "reading_75_percentile",
        "SAT Math 25th percentile score": "math_25_percentile",
        "SAT Math 75th percentile score": "math_75_percentile",
        "SAT Writing 25th percentile score": "writing_25_percentile",
        "SAT Writing 75th percentile score": "writing_75_percentile",
        "ACT Composite 25th percentile score": "ACT_25_percentile",
        "ACT Composite 75th percentile score": "ACT_75_percentile"}

    college_adm = rename_columns(college_adm, columns_mapping)
    uni_glb_rnk = clean_data(uni_glb_rnk, ["institution"])
    college_adm = clean_data(college_adm, ["Name"])
    # Select required columns
    uni_glb_rnk_selected = uni_glb_rnk.select("institution", "world_rank", "national_rank", "country", "year")
    college_adm_selected = college_adm.select("Name", "Applicants_total", "Admissions_total", "Enrolled_total",
                                              "State_abbreviation")
    college_adm_sat_scores_selected = college_adm.select("Name", "reading_25_percentile", "reading_75_percentile",
                                                         "math_25_percentile", "math_75_percentile",
                                                         "writing_25_percentile", "writing_75_percentile")

    # Get the last known global ranking for each institution
    last_known_global_ranking = get_last_known_global_ranking(uni_glb_rnk_selected)
    # Join admissions data with the last known global ranking data
    admissions_with_ranking = join_admissions_with_ranking(college_adm_selected, last_known_global_ranking)

    # Write dataset to a parquet file
    save_as(admissions_with_ranking, "etl/output/global_rnk_trends", "parquet")

    # Top 1000 universities
    pivot_by_year = filter_and_pivot_ranking(uni_glb_rnk_selected)
    save_as(pivot_by_year, "etl/output/top_universities_per_year", "parquet")

    top_n_ranked_universities = filter_top_n_ranked_universities(last_known_global_ranking, 1000)
    save_as(top_n_ranked_universities, "etl/output/top_n_ranked_universities", "parquet")

    # Trends by state with respect to the number of applicants vs admissions
    state_trends = calculate_state_trends(college_adm_selected)
    save_as(state_trends, "etl/output/state_trends", "parquet")

    # Trends by state with respect to the number of applicants vs admissions per year
    state_trends_global_rank_year_df = calculate_state_trends_global(admissions_with_ranking)
    save_as(state_trends_global_rank_year_df, "etl/output/state_trends_rank", "parquet")

    # Recommended Colleges to apply and list of colleges that has high probability of giving admissions
    applicant_score = {
        "sat_reading_score": 500,
        "sat_writing_score": 550,
        "sat_math_score": 650
    }
    sat_scores = college_adm_sat_scores_selected.dropna()

    recommended, high_probability = get_colleges_apply_vs_high_probability(applicant_score, sat_scores)
    save_as(recommended, "etl/output/recommended", "parquet")
    save_as(high_probability, "etl/output/high_probability", "parquet")

