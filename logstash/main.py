import threading
import os
import requests
import zipfile
from io import BytesIO
from time import sleep
import sys
import os
from pyspark.sql.functions import col, struct, array_distinct
from pyspark.sql import SparkSession
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser
from pyspark.sql.functions import col, concat_ws
import glob
import shutil
import time, datetime, pytz

#Get download file link from web
LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
DOWNLOAD_FOLDER = "./csv"
LOG_FILE = "./logs/log.txt"
SCRAPING_LOG_FILE = "./logs/scraping_log.txt"
INGESTION_LOG_FILE = "./logs/ingestion_log.txt"
TIMESTAMP_LOG_FILE = "./logs/timestamp_log.txt"

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs("./logs", exist_ok=True)

with open(LOG_FILE, "w") as f:
    f.write("")

def write(content, file):
    '''
    Logs messages to a text file, keeping only the first 500 lines of log text.
    '''
    if not content:
        return
    
    timezone = pytz.timezone("Asia/Singapore")  # or "Asia/Shanghai", "Asia/Manila", etc.
    current_time_gmt8 = datetime.datetime.now(timezone)
    current_time = current_time_gmt8.strftime("%Y-%m-%d %H:%M:%S") + ": "
    with open(file, "a", encoding="utf-8") as f:
        f.write(current_time + content + "\n")

def get_latest_gdelt_links():
    """
    Fetches the latest update file and extracts the download URLs.
    :return: List of CSV ZIP file URLs
    """
    response = requests.get(LAST_UPDATE_URL)
    
    if response.status_code != 200:
        write("Failed to fetch lastupdate.txt",LOG_FILE)
        return []
    
    lines = response.text.strip().split("\n")
    urls = [line.split()[-1] for line in lines if line.endswith(".zip")]
    
    return urls

def download_and_extract(url):
    """
    Downloads a ZIP file from the given URL and extracts CSV files.
    :param url: The URL to fetch the zip files from
    """
    file_name = url.split("/")[-1]
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        write(f"Failed to get {url}",LOG_FILE)
        write(f"Failed to get {url}",SCRAPING_LOG_FILE)
        return
    
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    
    for file in zip_file.namelist():
        if file.lower().endswith("gkg.csv"):
            write(f"Extracting: {file}",LOG_FILE)
            write(f"Extracting: {file}",SCRAPING_LOG_FILE)
            zip_file.extract(file, DOWNLOAD_FOLDER)
            write(f"Extraction completed: {file_name}", LOG_FILE)
            write(f"Extraction completed: {file_name}",SCRAPING_LOG_FILE)

def run_pipeline(raw_file, json_output):
    """
    Reads a raw GKG CSV file, transforms each line using gkg_parser,
    creates a Spark DataFrame with the defined schema, and writes the output as a single
    JSON file.
    :param raw_file: Directory containing raw CSV file
    :param json_output: Path where JSON files should be output into
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
    
    # Changing column names
    column_names = ["V21ShareImg", "V21SocImage", "V2DocId", "V21RelImg", "V21Date"]
    for col_name in column_names:
        df_transformed = df_transformed.withColumn(col_name, col(f"{col_name}.{col_name}"))

    # Reduce to a single partition so that we get one output file.
    df_transformed.coalesce(1).write.mode("overwrite").json(json_output)
    print(f"Pipeline completed. Single JSON output written to {json_output}")

    # Locates a JSON output file
    json_part_file = glob.glob(os.path.join(json_output, "part-00000-*.json"))[0]

    # Constructing new JSON file name
    date_part = str((raw_file.split('/')[2].split('.'))[0])
    new_file_name = f"{date_part}.json"

    # Moves JSON file, and copies renamed file for ingestion
    shutil.move(json_part_file, os.path.join(json_output, new_file_name))
    move_json_to_ingest(os.path.join(json_output, new_file_name))

    spark.stop()

def process_downloaded_files():
    '''
    Process downloaded CSV files, converting them into JSON format,
    and delete the processed CSV file.
    '''
    # Create the JSON subfolder if it does not exist.
    logstash_path = "./logstash_ingest_data/json"
    os.makedirs(logstash_path, exist_ok=True)

    # Source directory for CSV files
    src_path = "./csv"
    # List only the filenames in the CSV folder
    files = os.listdir(src_path)

    for file in files:
        if file.endswith(".csv"):
            # Build the full path to the file
            raw_file_path = os.path.join(src_path, file)
            # Create the JSON output path by replacing .csv with .json
            json_output_path = raw_file_path.replace(".csv", ".json")
            write(f"Processing file: {file}", LOG_FILE)
            write(f"Processing file: {file}", INGESTION_LOG_FILE)
            run_pipeline(raw_file_path, json_output_path)
            
            # Remove the CSV file using its full path
            os.remove(raw_file_path)

    age_threshold = 24 * 60 * 60
    current_time = time.time()

    # Cleaning JSON folders
    directory = "./logstash_ingest_data/json"
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        # Check if it's a file
        if os.path.isfile(file_path) and file_path.endswith(".json"):
                # Get the last modification time
                file_mod_time = os.path.getmtime(file_path)

                # Deletes file if it is older than 24 hours
                if (current_time - file_mod_time) > age_threshold:
                    write(f"Deleting: {file_path}", LOG_FILE)
                    os.remove(file_path)

    # Cleaning processed files
    for folder_path in os.listdir(src_path):
        if ".gkg.json" in folder_path: shutil.rmtree(os.path.join(src_path, folder_path))

def move_json_to_ingest(file_path):
    '''
    Moves the JSON file over to the json subfolder in logstash_ingest_data.
    :param file_path: File path of target file to be shifted.
    '''
    logstash_path = "./logstash_ingest_data/json"
    os.makedirs(logstash_path, exist_ok=True)
    
    # Only copy the .json file
    if os.path.isfile(file_path) and file_path.endswith(".json"):
        # Get the filename from the full path and copy to the target directory
        target_path = os.path.join(logstash_path, os.path.basename(file_path))
        shutil.move(file_path, target_path)
        print(f"Copied {file_path} to {target_path}")
    else:
        print(f"Invalid file: {file_path} (Not a .json file or file doesn't exist)")

def server_scrape():
    '''
    Scrapes data off the GDELT server,
    and downloads the resultant CSV files every 15 minutes.
    '''
    while True:
        try:
            csv_zip_urls = get_latest_gdelt_links()

            if not csv_zip_urls:
                write("No CSV ZIP links found in lastupdate.txt",LOG_FILE)
                write("No CSV ZIP links found in lastupdate.txt",SCRAPING_LOG_FILE)
            else:
                write(f"Found {len(csv_zip_urls)} files to download...",LOG_FILE)
                write(f"Found {len(csv_zip_urls)} files to download...",SCRAPING_LOG_FILE)
                for url in csv_zip_urls:
                    download_and_extract(url)

            write("\n", TIMESTAMP_LOG_FILE)

            # Repeats the scraping and downloading process every 15 min
            sleep(15*60)     

        except:
            write(f"Error: {url} cannot be successfully downloaded!",LOG_FILE)
            write(f"Error: {url} cannot be successfully downloaded!",SCRAPING_LOG_FILE)

def json_convert():
    '''
    Checks for new CSV files, converts the data to JSON format every 2 sec.
    Deletes CSV files once converted into JSON files.
    '''
    while True:
        process_downloaded_files()

############################################# Main #############################################
if __name__ == "__main__":
    thread1 = threading.Thread(target=server_scrape, daemon=True)
    thread2 = threading.Thread(target=json_convert, daemon=True)
    
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
