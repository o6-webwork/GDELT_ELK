import sys
import os

# Ensure the "src" directory is in sys.path for the driver.
src_path = "./csv/"
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pyspark.sql import SparkSession
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser

def run_pipeline(raw_file, parquet_output, json_output):
    """
    Reads a raw GKG CSV file, transforms each line using gkg_parser,
    creates a Spark DataFrame with the defined schema, and writes the output as a single
    Parquet file and a single JSON file.
    """
    spark = SparkSession.builder.appName("Standalone GKG ETL").getOrCreate()

    # Read the raw file as an RDD of lines.
    rdd = spark.sparkContext.textFile(raw_file)
    
    # Apply the transformation using gkg_parser (which splits each line into 27 fields).
    parsed_rdd = rdd.map(lambda line: gkg_parser(line))
    
    # Convert the transformed RDD to a DataFrame using the defined gkg_schema.
    df = spark.createDataFrame(parsed_rdd, schema=gkg_schema)
    
    # Reduce to a single partition so that we get one output file.
    df_single = df.coalesce(1)
    
    # Write as a single Parquet file.
    df_single.write.mode("overwrite").parquet(parquet_output)
    print(f"Pipeline completed. Single Parquet output written to {parquet_output}")
    
    # Write as a single JSON file.
    df_single.write.mode("overwrite").json(json_output)
    print(f"Pipeline completed. Single JSON output written to {json_output}")
    
    spark.stop()

if __name__ == "__main__":
    raw_file_path = src_path + "20250303074500.gkg.csv"
    parquet_output_path = "transformed_gkg.parquet"
    json_output_path = "transformed_gkg.json"
    run_pipeline(raw_file_path, parquet_output_path, json_output_path)
