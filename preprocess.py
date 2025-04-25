from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SearchLogProcessor") \
    .getOrCreate()

# Read the search log file
search_log = spark.sparkContext.textFile("searchLog.csv")

# Function to parse each line
def parse_line(line):
    results = []
    # Extract search term
    if not line.startswith("searchTerm:"):
        return results
    
    parts = line[len("searchTerm:"):].strip().split(", ", 1)
    if len(parts) != 2:
        return results
    
    term = parts[0].strip()
    url_clicks = parts[1].split("~")
    
    for url_click in url_clicks:
        if ":" in url_click:
            url, clicks = url_click.split(":", 1)
            try:
                clicks = int(clicks)
                results.append((term, url, clicks))
            except ValueError:
                continue
    
    return results

# Parse the log file and extract records
records = search_log.flatMap(parse_line)

# Define schema for the DataFrame
schema = StructType([
    StructField("term", StringType(), True),
    StructField("url", StringType(), True),
    StructField("clicks", IntegerType(), True)
])

# Create DataFrame from RDD
df = spark.createDataFrame(records, schema)

# Save DataFrame as JSON
df.write.mode("overwrite").json("processed_data")

# Stop the Spark session
spark.stop()