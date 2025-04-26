from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SearchLogProcessor") \
    .getOrCreate()

search_log_files = spark.sparkContext.wholeTextFiles("searchLog.csv")
search_log_content = search_log_files.map(lambda x: x[1])

def split_into_logical_lines(content):
    cleaned_content = content.replace("\n", " ")
    parts = cleaned_content.split("searchTerm:")
    logical_lines = ["searchTerm:" + part.strip() for part in parts[1:] if part.strip()]
    return logical_lines

# Get logical lines containing complete search terms
logical_lines = search_log_content.flatMap(split_into_logical_lines)

# Function to parse each logical line
def parse_line(line):
    results = []
    line = line.strip()
    if not line.startswith("searchTerm:"):
        return results
    
    content = line[len("searchTerm:"):].strip()
    
    parts = content.split(",", 1)
    if len(parts) != 2:
        return results
    
    term = parts[0].strip()
    url_clicks_part = parts[1].strip()
    
    url_clicks = url_clicks_part.split("~")
    
    for url_click in url_clicks:
        url_click = url_click.strip()
        if ":" in url_click:
            url_parts = url_click.split(":", 1)
            if len(url_parts) == 2:
                url, clicks = url_parts
                url = url.strip()
                try:
                    clicks = int(clicks.strip())
                    results.append((term, url, clicks))
                except ValueError:
                    continue
    
    return results

# Parse the log file and extract records
records = logical_lines.flatMap(parse_line)

# Define schema for the DataFrame
schema = StructType([
    StructField("term", StringType(), True),
    StructField("url", StringType(), True),
    StructField("clicks", IntegerType(), True)
])

# Create DataFrame from RDD
df = spark.createDataFrame(records, schema)

# Sort the DataFrame to ensure consistent ordering
# First by term, then by clicks in descending order, then by url
df = df.orderBy("term", df.clicks.desc(), "url")

# Save DataFrame as JSON
df.write.mode("overwrite").json("processed_data")

# Stop the Spark session
spark.stop()