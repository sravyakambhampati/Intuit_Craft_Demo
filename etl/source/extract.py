from pyspark.sql import DataFrame


def extract_from_sources(spark, file_path: str, file_format: str, options=None) -> DataFrame:
    """
    Read data from different file formats

    Args:
        spark: Spark session
        file_path (str): The path to the file.
        file_format (str): The format of the file (e.g., 'csv', 'orc', 'parquet', 'avro').
        options (dict): Optional options to pass to the reader.

    Returns:
        DataFrame: A PySpark DataFrame containing the data.
    """
    if file_format == "csv":
        return spark.read.csv(file_path, **options)
    elif file_format in ("orc", "parquet", "avro"):
        return spark.read.format(file_format).load(file_path)
    else:
        raise ValueError
