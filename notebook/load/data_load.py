import os
from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str = "DataLoadApp") -> SparkSession:
    """
    Create and return a SparkSession using Azure best practices.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return builder.getOrCreate()

def load_data_from_adls(spark: SparkSession, file_path: str, file_format: str = "parquet") -> DataFrame:
    """
    Load data from Azure Data Lake Storage (ADLS) into a Spark DataFrame.
    Args:
        spark: SparkSession object.
        file_path: ADLS path to the data file.
        file_format: Format of the data file (default: parquet).
    Returns:
        DataFrame containing the loaded data.
    """
    if file_format == "parquet":
        df = spark.read.parquet(file_path)
    elif file_format == "csv":
        df = spark.read.option("header", "true").csv(file_path)
    elif file_format == "delta":
        df = spark.read.format("delta").load(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    return df

def save_data_to_adls(df: DataFrame, output_path: str, file_format: str = "parquet", mode: str = "overwrite"):
    """
    Save a Spark DataFrame to Azure Data Lake Storage (ADLS).
    Args:
        df: DataFrame to save.
        output_path: ADLS path to save the data.
        file_format: Format to save the data (default: parquet).
        mode: Save mode (default: overwrite).
    """
    if file_format == "parquet":
        df.write.mode(mode).parquet(output_path)
    elif file_format == "csv":
        df.write.mode(mode).option("header", "true").csv(output_path)
    elif file_format == "delta":
        df.write.format("delta").mode(mode).save(output_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

# Example usage (uncomment for Databricks notebook)
# spark = get_spark_session()
# df = load_data_from_adls(spark, "abfss://container@account.dfs.core.windows.net/path/to/data", "parquet")
# save_data_to_adls(df, "abfss://container@account.dfs.core.windows.net/path/to/output", "delta")