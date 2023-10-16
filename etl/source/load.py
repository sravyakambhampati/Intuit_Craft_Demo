def save_as(data, output_path, file_format="parquet"):
    """
    Save data as Parquet or ORC
    :param data: The pyspark dataframe
    :param output_path: Output Location to save parquet or orc file
    :param file_format: parquet or orc
    :return: None
    """

    if file_format == "parquet":
        data.write.parquet(output_path, mode="overwrite")
    elif file_format == "orc":
        data.write.orc(output_path, mode="overwrite")
