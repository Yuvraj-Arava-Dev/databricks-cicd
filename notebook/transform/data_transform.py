from pyspark.sql import DataFrame, SparkSession

# data_transform.py

import pyspark.sql.functions as F

def transform_data(df: DataFrame) -> DataFrame:
    """
    Applies basic data transformations to the input DataFrame.
    Example transformations:
      - Trim string columns
      - Convert column names to lowercase
      - Remove duplicate rows

    Args:
        df (DataFrame): Input Spark DataFrame.

    Returns:
        DataFrame: Transformed Spark DataFrame.
    """
    # Trim all string columns
    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == 'string']
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    # Convert all column names to lowercase
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())

    # Remove duplicate rows
    df = df.dropDuplicates()

    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataTransform").getOrCreate()
    # Example usage: read a CSV, transform, and write back
    input_path = "dbfs:/mnt/input/data.csv"
    output_path = "dbfs:/mnt/output/data_transformed.csv"

    df = spark.read.option("header", True).csv(input_path)
    df_transformed = transform_data(df)
    df_transformed.write.mode("overwrite").option("header", True).csv(output_path)
    spark.stop()