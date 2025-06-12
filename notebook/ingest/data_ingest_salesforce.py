from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SampleDataIngestSalesforce") \
    .getOrCreate()

# Sample data (simulate Salesforce data)
data = [
    ("001", "Alice", "alice@example.com", 1000),
    ("002", "Bob", "bob@example.com", 1500),
    ("003", "Charlie", "charlie@example.com", 2000)
]

columns = ["Id", "Name", "Email", "Revenue"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# Stop Spark session
spark.stop()