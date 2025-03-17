import os
import requests
import zipfile
from io import BytesIO
from time import sleep
from pathlib import Path
import sys
import os
from pyspark.sql.functions import col, struct, array_distinct
import json
from pyspark.sql import SparkSession
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import glob
import shutil
import time
#Get download file link from web
LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
DOWNLOAD_FOLDER = "./csv"
LOG_FILE = "./logs/log.txt"

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs("./logs", exist_ok=True)

def write(content):
    """Write log data into log file."""
    with open(LOG_FILE, "a") as f:
        f.write(content + "\n")

def get_latest_gdelt_links():
    """
    Fetches the latest update file and extracts the download URLs.
    :return: List of CSV ZIP file URLs
    """
    response = requests.get(LAST_UPDATE_URL)
    
    if response.status_code != 200:
        write("Failed to fetch lastupdate.txt")
        return []
    
    lines = response.text.strip().split("\n")
    urls = [line.split()[-1] for line in lines if line.endswith(".zip")]
    
    return urls

def download_and_extract(url, out):
    """
    Downloads a ZIP file from the given URL and extracts CSV files.
    :param url: The URL to download
    :return: List of existing file names
    """
    file_name = url.split("/")[-1]
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        write(f"Failed to get {url}")
        return
    
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    
    for file in zip_file.namelist():
        if file.lower().endswith("gkg.csv") and file not in out:
            write(f"Extracting: {file}")
            zip_file.extract(file, DOWNLOAD_FOLDER)
            write(f"Completed: {file_name}")
            out.append(file)

    return list(set(out))


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

    # Concatenate GkgRecordId.Date and GkgRecordId.NumberInBatch with "-"
    df_transformed = df.withColumn(
        "RecordId",
        concat_ws("-", col("GkgRecordId.Date").cast("string"), col("GkgRecordId.NumberInBatch").cast("string"))
    )

    df_transformed = df_transformed.drop("V1Counts") 
    df_transformed = df_transformed.drop("V1Locations")
    df_transformed = df_transformed.drop("V1Orgs")
    df_transformed = df_transformed.drop("V1Persons")
    df_transformed = df_transformed.drop("V1Themes")
    df_transformed = df_transformed.drop("V21Amounts")
    df_transformed = df_transformed.drop("V21Counts")
    df_transformed = df_transformed.drop("V21EnhancedDates")


    df_transformed = df_transformed.withColumn(
        "V15Tone",
        struct(
            col("V15Tone.Tone"),
            col("V15Tone.PositiveScore"),
            col("V15Tone.NegativeScore"),
            col("V15Tone.Polarity"),
            col("V15Tone.ActivityRefDensity"),
            col("V15Tone.SelfGroupRefDensity")  # Removed 'WordCount'
        )
    )

    df_transformed = df_transformed.withColumn(
        "V21Quotations",
        struct(
            col("V21Quotations.Verb"),
            col("V21Quotations.Quote")  # Removed 'WordCount'
        )
    )

    df_transformed = df_transformed.withColumn(
        "V2Persons",
        struct(
            col("V2Persons.V1Person") # Removed 'WordCount'
        )
    )

    df_transformed = df_transformed.withColumn(
        "V2Orgs",
        struct(
            col("V2Orgs.V1Org")  # Removed 'WordCount'
        )
    )

    df_transformed = df_transformed.withColumn(
        "V2Locations",
        struct(
            col("V2Locations.FullName"),
            col("V2Locations.CountryCode"),
            col("V2Locations.ADM1Code"),
            col("V2Locations.ADM2Code"),
            col("V2Locations.LocationLatitude"),
            col("V2Locations.LocationLongitude"),
            col("V2Locations.FeatureId")  # Removed 'WordCount'
        )
    )

    df_transformed = df_transformed.withColumn(
        "V2EnhancedThemes",
        struct(
            col("V2EnhancedThemes.V2Theme")  # Removed 'WordCount'
        )
    )
    
    # Remove duplicates
    df_transformed = df_transformed.withColumn(
        "V2Locations",
        struct(
            array_distinct(col("V2Locations.FullName")).alias("FullName"),
            array_distinct(col("V2Locations.CountryCode")).alias("CountryCode"),
            array_distinct(col("V2Locations.ADM1Code")).alias("ADM1Code"),
            array_distinct(col("V2Locations.ADM2Code")).alias("ADM2Code"),
            array_distinct(col("V2Locations.LocationLatitude")).alias("LocationLatitude"),
            array_distinct(col("V2Locations.LocationLongitude")).alias("LocationLongitude"),
            array_distinct(col("V2Locations.FeatureId")).alias("FeatureId")
        )
    )
    df_transformed = df_transformed.withColumn(
        "V2Persons",
        struct(
            array_distinct(col("V2Persons.V1Person")).alias("V1Person"),
        )
    )

    # df_transformed = df_transformed.withColumn(
    #     "V21Counts",
    #     struct(
    #         array_distinct(col("V21Counts.CountType")).alias("CountType"),
    #         array_distinct(col("V21Counts.Count")).alias("Count"),
    #         array_distinct(col("V21Counts.ObjectType")).alias("ObjectType"),
    #         array_distinct(col("V21Counts.LocationType")).alias("LocationType"),
    #         array_distinct(col("V21Counts.FullName")).alias("FullName"),
    #         array_distinct(col("V21Counts.CountryCode")).alias("CountryCode"),
    #         array_distinct(col("V21Counts.ADM1Code")).alias("ADM1Code"),
    #         array_distinct(col("V21Counts.LocationLatitude")).alias("LocationLatitude"),
    #         array_distinct(col("V21Counts.LocationLongitude")).alias("LocationLongitude"),
    #         array_distinct(col("V21Counts.FeatureId")).alias("FeatureId"),
    #         array_distinct(col("V21Counts.CharOffset")).alias("CharOffset"),
    #     )
    # )

    df_transformed = df_transformed.withColumn(
        "V2EnhancedThemes",
        struct(
            array_distinct(col("V2EnhancedThemes.V2Theme")).alias("V2Theme"),
        )
    )

    df_transformed = df_transformed.withColumn(
        "V2Orgs",
        struct(
            array_distinct(col("V2Orgs.V1Org")).alias("V1Org"),
        )
    )

    df_transformed = df_transformed.withColumn(
        "V2GCAM",
        struct(
            array_distinct(col("V2GCAM.DictionaryDimId")).alias("DictionaryDimId"),
        )
    )

    df_transformed = df_transformed.withColumn(
        "V21Quotations",
        struct(
            array_distinct(col("V21Quotations.Verb")).alias("Verb"),
            array_distinct(col("V21Quotations.Quote")).alias("Quote"),
        )
    )


    df_transformed = df_transformed.withColumn(
        "V21AllNames",
        struct(
            array_distinct(col("V21AllNames.Name")).alias("Name"),
        )
    )
    
    # change column names
    column_names = ["V21ShareImg", "V21SocImage", "V2DocId", "V21RelImg", "V21Date"]
    for col_name in column_names:
        df_transformed = df_transformed.withColumn(col_name, col(f"{col_name}.{col_name}"))

    # Reduce to a single partition so that we get one output file.
    df_transformed.coalesce(1).write.mode("overwrite").json(json_output)
    print(f"Pipeline completed. Single JSON output written to {json_output}")

    json_part_file = glob.glob(os.path.join(json_output, "part-00000-*.json"))[0]
    date_part = str((raw_file.split('/')[2].split('.'))[0])
    new_file_name = f"{date_part}.json"
    shutil.move(json_part_file, os.path.join(json_output, new_file_name))
    cp_json_to_ingest(os.path.join(json_output, new_file_name))
    # # Write as a single Parquet file.
    # df_transformed.write.mode("overwrite").parquet(parquet_output)
    # print(f"Pipeline completed. Single Parquet output written to {parquet_output}")
    
    # Write as a single JSON file.

    spark.stop()

def process_downloaded_files(out):
    logstash_path = "./logstash_ingest_data/json"
    os.makedirs(logstash_path, exist_ok=True)  # Ensure the directory exists
    src_path = "./csv/"
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    for file in out:
        if file.endswith(".csv"):
            raw_file_path = os.path.join(src_path, file)
            parquet_output_path = raw_file_path.replace(".csv", ".parquet")
            json_output_path = raw_file_path.replace(".csv", ".json")
            
            write(f"Processing file: {raw_file_path}")
            run_pipeline(raw_file_path, parquet_output_path, json_output_path)

def cp_json_to_ingest(file_path):
    logstash_path = "./logstash_ingest_data/json"
    os.makedirs(logstash_path, exist_ok=True)
    
    # Only copy the .json file
    if os.path.isfile(file_path) and file_path.endswith(".json"):
        # Get the filename from the full path and copy to the target directory
        target_path = os.path.join(logstash_path, os.path.basename(file_path))
        shutil.copy(file_path, target_path)
        print(f"Copied {file_path} to {target_path}")
    else:
        print(f"Invalid file: {file_path} (Not a .json file or file doesn't exist)")

        
if __name__ == "__main__":
    out = []

    while True:
        csv_zip_urls = get_latest_gdelt_links()

        if not csv_zip_urls:
            write("No CSV ZIP links found in lastupdate.txt")
        else:
            write(f"Found {len(csv_zip_urls)} files to download...\n")
            for url in csv_zip_urls:
                out = download_and_extract(url, out)

        write("All files downloaded and extracted in the 'downloads' folder.")
        process_downloaded_files(out)
        sleep(15*60) # every 15 minutes

        age_threshold = 24 * 60 * 60  # 86400 seconds - 24 hours

        # Get the current time
        current_time = time.time()

        # Loop through files in the directory
        directory = "./logstash_ingest_data/json"
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            # Check if it's a file (not a directory)
            if os.path.isfile(file_path):
                if file_path.endswith(".json"):
                    # Get the last modification time
                    file_mod_time = os.path.getmtime(file_path)

                    # Check if the file is older than 24 hours
                    if (current_time - file_mod_time) > age_threshold:
                        print(f"Deleting: {file_path}")
                        os.remove(file_path)  # Delete the file