def extract_from_sources(spark, file_path, file_format, options=None):
    """
    Read data from different file formats

    Args:
        file_path (str): The path to the file.
        file_format (str): The format of the file (e.g., 'csv', 'orc', 'parquet', 'avro').
        options (dict): Optional options to pass to the reader.

    Returns:
        DataFrame: A PySpark DataFrame containing the data.
    """
    if file_format == "csv":
        return spark.read.csv(file_path, **options)
    else:
        return spark.read.format(file_format).load(file_path)

