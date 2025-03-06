from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, lit, count
from pyspark.sql.types import StructType, StructField, StringType, LongType
import functools
import os

# Set up PostgreSQL connection parameters
POSTGRES_URL = "jdbc:postgresql://postgres:5432/github_repos"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "password"

# Create Spark Session
spark = SparkSession.builder \
    .appName("GitHubReposAnalysis") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Path to JSON files (Modify based on mounted volume in Docker)
json_path = "/opt/spark/data"

# Define JSON schema
github_repo_schema = StructType([
    StructField("id", LongType(), True),
    StructField("repo_name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created", StringType(), True),
    StructField("language", StringType(), True),
    StructField("type", StringType(), True),
    StructField("username", StringType(), True),
    StructField("stars", LongType(), True),
    StructField("forks", LongType(), True),
    StructField("subscribers", LongType(), True),
    StructField("open_issues", LongType(), True),
    StructField("topics", StringType(), True),
    StructField("search_term", StringType(), True)
])

# Function to read all JSON files
def read_json_files(path):
    """
    Read all JSON files from the specified path.
    """
    files = [f for f in os.listdir(path) if f.endswith('.json')]
    
    if not files:
        raise ValueError("No JSON files found in the specified path")

    dfs = []
    for file in files:
        search_term = file.replace('.json', '')
        try:
            df = spark.read.schema(github_repo_schema).json(os.path.join(path, file))
            df = df.withColumn("search_term", lit(search_term))
            dfs.append(df)
        except Exception as e:
            print(f"Error reading file {file}: {e}")
    
    return functools.reduce(DataFrame.unionByName, dfs) if dfs else None

# Load JSON data
repos_df = read_json_files(json_path)

if repos_df is None:
    print("No data loaded, exiting.")
    exit()

# 1. Programming Languages Table
programming_lang_df = repos_df.groupBy("language") \
    .agg(
        col("language").alias("language_name"),
        count("*").alias("repo_count")
    ).filter(col("language").isNotNull()) \
    .select("language_name", "repo_count")

# 2. Organizations Stars Table
organizations_stars_df = repos_df.filter(col("type") == "Organization") \
    .groupBy("username") \
    .agg(
        col("username").alias("organization_name"),
        spark_sum("stars").alias("total_stars")
    ).select("organization_name", "total_stars")

# 3. Search Terms Relevance Table
search_terms_relevance_df = repos_df.groupBy("search_term") \
    .agg(
        spark_sum(1.5 * col("forks") + 1.32 * col("subscribers") + 1.04 * col("stars")).alias("relevance_score")
    )

# Function to write DataFrame to PostgreSQL
def write_to_postgres(df, table_name):
    """
    Write a DataFrame to PostgreSQL.
    """
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

# Write data to PostgreSQL tables
write_to_postgres(programming_lang_df, "programming_lang")
write_to_postgres(organizations_stars_df, "organizations_stars")
write_to_postgres(search_terms_relevance_df, "search_terms_relevance")

print("Data successfully written to PostgreSQL!")
