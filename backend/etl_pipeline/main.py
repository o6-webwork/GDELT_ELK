import threading
import os
import requests
import zipfile
from io import BytesIO
from time import sleep
import sys
import glob
import shutil
import time
import datetime
import pytz
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, array_distinct, concat_ws
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType # Import StructType if used directly in schema definition

# Assuming schemas are in the same directory structure or installed package
# It's better practice to handle imports relative to the project structure
# If run as a script, ensure PYTHONPATH is set or use relative imports carefully.
try:
    from schemas.gkg_schema import gkg_schema
    from etl.parse_gkg import gkg_parser # Assuming gkg_parser is correctly defined elsewhere
except ImportError:
    # Fallback for running directly if structure is flat or PYTHONPATH isn't set
    logging.warning("Could not import schemas/etl using package structure, attempting direct import.")
    # Add logic here if schemas/etl are in the same directory or handle error
    # This part depends heavily on how you run this script.
    # For now, assume the imports work as intended in the container.
    from schemas.gkg_schema import gkg_schema
    from etl.parse_gkg import gkg_parser


# --- Configuration ---
# Use environment variables with defaults
LAST_UPDATE_URL = os.environ.get("GDELT_LAST_UPDATE_URL", "http://data.gdeltproject.org/gdeltv2/lastupdate.txt")
# These paths should match the volumes mounted in docker-compose for the etl_processor service
DOWNLOAD_FOLDER = os.environ.get("GDELT_DOWNLOAD_FOLDER", "/app/csv") # Internal container path
LOG_FOLDER = os.environ.get("GDELT_LOG_FOLDER", "/app/logs") # Internal container path
JSON_INGEST_FOLDER = os.environ.get("GDELT_JSON_INGEST_FOLDER", "/app/logstash_ingest_data/json") # Internal container path

SPARK_APP_NAME = "GDELT_GKG_ETL"
DOWNLOAD_INTERVAL_SECONDS = 15 * 60 # 15 minutes
FILE_CLEANUP_INTERVAL_SECONDS = 6 * 60 * 60 # Every 6 hours
FILE_AGE_THRESHOLD_SECONDS = 7 * 24 * 60 * 60 # 7 days

# --- Logging Setup ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
LOG_FILE = os.path.join(LOG_FOLDER, "gdelt_etl.log")
TIMESTAMP_LOG_FILE = os.path.join(LOG_FOLDER, "timestamp.log")

# Ensure log directory exists (important when run in container)
os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(JSON_INGEST_FOLDER, exist_ok=True)


logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    handlers=[
                        logging.FileHandler(LOG_FILE),
                        logging.StreamHandler(sys.stdout) # Log to console (Docker logs) as well
                    ])

# --- Spark Session Management ---
spark = None # Initialize spark variable
try:
    spark = SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate()
    logging.info(f"SparkSession '{SPARK_APP_NAME}' created successfully.")
except Exception as e:
    logging.critical(f"Failed to create SparkSession: {e}", exc_info=True)
    # Decide if the script should exit if Spark fails. Usually yes for an ETL script.
    sys.exit(1)

# --- Helper Functions ---
def get_latest_gdelt_links():
    """Fetches the latest update file and extracts the download URLs."""
    try:
        response = requests.get(LAST_UPDATE_URL, timeout=30)
        response.raise_for_status()
        lines = response.text.strip().split("\n")
        urls = [line.split()[-1] for line in lines if line.endswith(".zip")]
        logging.info(f"Found {len(urls)} potential GDELT file URLs.")
        return urls
    except requests.exceptions.RequestException as e:
        logging.error(f"Extraction failure: failed to fetch {LAST_UPDATE_URL}. Error: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error fetching GDELT links: {e}", exc_info=True)
        return []

def download_and_extract(url):
    """Downloads a ZIP file from the given URL and extracts GKG CSV files."""
    extracted_files = []
    try:
        logging.info(f"Attempting download: {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            for file_info in zip_file.infolist():
                if file_info.filename.lower().endswith("gkg.csv") and not file_info.is_dir():
                    safe_filename = os.path.basename(file_info.filename)
                    if safe_filename:
                        target_path = os.path.join(DOWNLOAD_FOLDER, safe_filename)
                        logging.info(f"Extracting: {safe_filename} to {DOWNLOAD_FOLDER}")
                        # Extract to a temporary location within the volume first
                        # to handle potential permission issues or complex paths in zip
                        temp_extract_path = os.path.join(DOWNLOAD_FOLDER, f"temp_{safe_filename}")
                        zip_file.extract(file_info, path=DOWNLOAD_FOLDER) # Extract preserving structure initially
                        extracted_full_path = os.path.join(DOWNLOAD_FOLDER, file_info.filename)

                        # Move the extracted file to the final target path if different
                        if os.path.exists(extracted_full_path):
                            shutil.move(extracted_full_path, target_path)
                            extracted_files.append(target_path)
                            logging.info(f"Extracted successfully: {safe_filename}")
                        else:
                             logging.error(f"Extracted file not found at expected path: {extracted_full_path}")

                    else:
                        logging.warning(f"Skipping potentially unsafe filename in zip: {file_info.filename}")
            return extracted_files
    except requests.exceptions.RequestException as e:
        logging.error(f"Download/Extraction failure for {url}. Request Error: {e}")
    except zipfile.BadZipFile:
        logging.error(f"Download/Extraction failure for {url}. Invalid ZIP file.")
    except IOError as e:
         logging.error(f"Download/Extraction failure for {url}. File I/O error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error downloading/extracting {url}: {e}", exc_info=True)
    return []

def transform_gkg_dataframe(df):
    """Applies transformations to the raw GKG DataFrame."""
    df_transformed = df.withColumn(
        "RecordId",
        concat_ws("-", col("GkgRecordId.Date").cast("string"), col("GkgRecordId.NumberInBatch").cast("string"))
    )
    cols_to_drop = [
        "V1Counts", "V1Locations", "V1Orgs", "V1Persons", "V1Themes",
        "V21Amounts", "V21Counts", "V21EnhancedDates"
    ]
    df_transformed = df_transformed.drop(*cols_to_drop)
    nested_structs_simple = {
        "V15Tone": ["Tone", "PositiveScore", "NegativeScore", "Polarity", "ActivityRefDensity", "SelfGroupRefDensity"],
        "V21Quotations": ["Verb", "Quote"], "V2Persons": ["V1Person"], "V2Orgs": ["V1Org"],
        "V2EnhancedThemes": ["V2Theme"]
    }
    for col_name, fields in nested_structs_simple.items():
        df_transformed = df_transformed.withColumn(col_name, struct(*(col(f"{col_name}.{f}") for f in fields)))
    df_transformed = df_transformed.withColumn("V2Locations", struct(
            col("V2Locations.FullName"), col("V2Locations.CountryCode"), col("V2Locations.ADM1Code"),
            col("V2Locations.ADM2Code"), col("V2Locations.LocationLatitude"), col("V2Locations.LocationLongitude"),
            col("V2Locations.FeatureId")))
    array_structs_distinct = {
        "V2Locations": ["FullName", "CountryCode", "ADM1Code", "ADM2Code", "LocationLatitude", "LocationLongitude", "FeatureId"],
        "V2Persons": ["V1Person"], "V2EnhancedThemes": ["V2Theme"], "V2Orgs": ["V1Org"],
        "V2GCAM": ["DictionaryDimId"], "V21Quotations": ["Verb", "Quote"], "V21AllNames": ["Name"]
    }
    for col_name, fields in array_structs_distinct.items():
         df_transformed = df_transformed.withColumn(col_name, struct(*(array_distinct(col(f"{col_name}.{f}")).alias(f) for f in fields)))
    cols_to_flatten = ["V21ShareImg", "V21SocImage", "V2DocId", "V21RelImg", "V21Date"]
    for col_name in cols_to_flatten:
         if col_name in df_transformed.columns and isinstance(df_transformed.schema[col_name].dataType, StructType):
              df_transformed = df_transformed.withColumn(col_name, col(f"{col_name}.{col_name}"))
    return df_transformed

def run_spark_pipeline(spark_session, raw_file_path):
    """Processes a single raw GKG CSV file using Spark."""
    base_filename = os.path.basename(raw_file_path)
    output_json_filename = base_filename.replace(".csv", ".json")
    # Use a temporary directory within the main download folder for Spark output parts
    temp_spark_output_dir = os.path.join(DOWNLOAD_FOLDER, f"{base_filename}_spark_temp")
    final_json_path = os.path.join(JSON_INGEST_FOLDER, output_json_filename)
    logging.info(f"Starting Spark pipeline for: {base_filename}")
    try:
        rdd = spark_session.sparkContext.textFile(raw_file_path)
        parsed_rdd = rdd.map(lambda line: gkg_parser(line))
        # Ensure gkg_parser returns tuples of the correct length or handles errors appropriately
        # Filter based on expected number of fields in the schema
        num_expected_fields = len(gkg_schema.fields)
        filtered_rdd = parsed_rdd.filter(lambda x: isinstance(x, (tuple, list)) and len(x) == num_expected_fields)
        # Log if records were filtered (optional)
        # original_count = parsed_rdd.count()
        # filtered_count = filtered_rdd.count()
        # if original_count != filtered_count:
        #    logging.warning(f"Filtered out {original_count - filtered_count} potentially corrupt records from {base_filename}")

        if filtered_rdd.isEmpty():
             logging.warning(f"No valid records found after parsing {base_filename}. Skipping.")
             return None

        df = spark_session.createDataFrame(filtered_rdd, schema=gkg_schema)
        df_transformed = transform_gkg_dataframe(df)

        if os.path.exists(temp_spark_output_dir):
            shutil.rmtree(temp_spark_output_dir)
            logging.info(f"Removed existing temp Spark output: {temp_spark_output_dir}")

        df_transformed.coalesce(1).write.mode("overwrite").json(temp_spark_output_dir)
        logging.info(f"Spark job finished. Intermediate JSON parts written to {temp_spark_output_dir}")

        json_part_files = glob.glob(os.path.join(temp_spark_output_dir, "part-*.json"))
        if not json_part_files:
            logging.error(f"No JSON part file found in {temp_spark_output_dir} for {base_filename}")
            return None

        source_part_file = json_part_files[0]
        shutil.move(source_part_file, final_json_path)
        logging.info(f"Moved and renamed JSON output to: {final_json_path}")
        return final_json_path
    except AnalysisException as e:
        logging.error(f"Spark Analysis Error processing {base_filename}: {e}", exc_info=True)
        return None
    except IOError as e:
        logging.error(f"File I/O Error during Spark processing or cleanup for {base_filename}: {e}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Unexpected Spark pipeline error for {base_filename}: {e}", exc_info=True)
        return None
    finally:
        try:
            if os.path.exists(temp_spark_output_dir):
                shutil.rmtree(temp_spark_output_dir)
                logging.info(f"Cleaned up temp Spark output directory: {temp_spark_output_dir}")
        except Exception as e:
            logging.warning(f"Could not clean up temp Spark directory {temp_spark_output_dir}: {e}")

def process_downloaded_files(spark_session):
    """Continuously monitors the download folder and processes new CSV files."""
    processed_files = set()
    while True:
        try:
            files_to_process = [
                os.path.join(DOWNLOAD_FOLDER, f)
                for f in os.listdir(DOWNLOAD_FOLDER)
                if f.lower().endswith(".csv") and os.path.join(DOWNLOAD_FOLDER, f) not in processed_files
            ]
            if not files_to_process:
                sleep(30)
                continue

            for csv_file_path in files_to_process:
                base_filename = os.path.basename(csv_file_path)
                logging.info(f"Found CSV file to process: {base_filename}")
                processed_files.add(csv_file_path)
                json_output_path = run_spark_pipeline(spark_session, csv_file_path)
                if json_output_path:
                    logging.info(f"Successfully processed {base_filename} -> {os.path.basename(json_output_path)}")
                    try:
                        os.remove(csv_file_path)
                        logging.info(f"Deleted processed CSV file: {base_filename}")
                    except OSError as e:
                        logging.error(f"Failed to delete processed CSV file {base_filename}: {e}")
                else:
                    logging.error(f"Failed to process CSV file: {base_filename}. Will not retry.")
        except Exception as e:
            logging.error(f"Error in file processing loop: {e}", exc_info=True)
            sleep(60)

def delete_old_files(directory, age_threshold_seconds):
    """Deletes files in a directory older than the specified threshold."""
    logging.info(f"Running cleanup task for directory: {directory}")
    try:
        current_time = time.time(); deleted_count = 0
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path):
                    file_mod_time = os.path.getmtime(file_path)
                    if (current_time - file_mod_time) > age_threshold_seconds:
                        os.remove(file_path)
                        logging.info(f"Deleted old file: {filename} (older than {age_threshold_seconds / 3600:.1f} hours)")
                        deleted_count += 1
            except OSError as e:
                logging.warning(f"Could not process or delete file {filename}: {e}")
        logging.info(f"Cleanup task finished for {directory}. Deleted {deleted_count} old files.")
    except Exception as e:
        logging.error(f"Error during cleanup task for {directory}: {e}", exc_info=True)

def periodic_cleanup():
    """Periodically runs the cleanup task."""
    while True:
        try:
            delete_old_files(JSON_INGEST_FOLDER, FILE_AGE_THRESHOLD_SECONDS)
            sleep(FILE_CLEANUP_INTERVAL_SECONDS)
        except Exception as e:
            logging.error(f"Error in periodic cleanup loop: {e}", exc_info=True)
            sleep(60)

def server_scrape():
    """Periodically scrapes GDELT server for new file links and downloads them."""
    while True:
        try:
            logging.info("Checking GDELT for latest update links...")
            csv_zip_urls = get_latest_gdelt_links()
            if not csv_zip_urls:
                logging.info("No new CSV ZIP links found.")
            else:
                logging.info(f"Found {len(csv_zip_urls)} files to potentially download...")
                for url in csv_zip_urls:
                    timestamp = url.split('/')[-1].split('.')[0]
                    potential_csv = os.path.join(DOWNLOAD_FOLDER, f"{timestamp}.gkg.csv")
                    potential_json = os.path.join(JSON_INGEST_FOLDER, f"{timestamp}.gkg.json")
                    if not os.path.exists(potential_csv) and not os.path.exists(potential_json):
                         download_and_extract(url)
                    else:
                         logging.info(f"Skipping download for {timestamp}, appears to exist locally.")
            # Log timestamp for monitoring
            try:
                timezone = pytz.timezone("Asia/Singapore") # Consider making timezone configurable
                timestamp_str = datetime.datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")
                with open(TIMESTAMP_LOG_FILE, "a") as f: f.write(timestamp_str + "\n")
            except IOError as e: logging.warning(f"Could not write to timestamp log: {e}")

            logging.info(f"Scraping cycle complete. Sleeping for {DOWNLOAD_INTERVAL_SECONDS} seconds.")
            sleep(DOWNLOAD_INTERVAL_SECONDS)
        except Exception as e:
            logging.error(f"Error in server scraping loop: {e}", exc_info=True)
            sleep(60)

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure SparkSession was created
    if spark is None:
         logging.critical("SparkSession failed to initialize. Exiting main script.")
         sys.exit(1)

    scraper_thread = threading.Thread(target=server_scrape, name="ScraperThread", daemon=True)
    processor_thread = threading.Thread(target=process_downloaded_files, args=(spark,), name="ProcessorThread", daemon=True)
    cleanup_thread = threading.Thread(target=periodic_cleanup, name="CleanupThread", daemon=True)

    logging.info("Starting GDELT ETL Threads...")
    scraper_thread.start()
    processor_thread.start()
    cleanup_thread.start()

    try:
        while True:
            # Basic thread health check
            if not scraper_thread.is_alive() or not processor_thread.is_alive():
                 logging.error("A critical worker thread (Scraper or Processor) has died. Exiting.")
                 break
            if not cleanup_thread.is_alive():
                 logging.warning("CleanupThread died unexpectedly. It might restart or indicate an issue.")
                 # Optionally attempt to restart cleanup_thread if needed
            sleep(60)
    except KeyboardInterrupt:
        logging.info("Shutdown signal received. Exiting.")
    finally:
        # Attempt graceful shutdown of Spark
        try:
            if spark:
                logging.info("Stopping SparkSession...")
                spark.stop()
                logging.info("SparkSession stopped.")
        except Exception as e:
            logging.error(f"Error stopping SparkSession: {e}", exc_info=True)
    logging.info("GDELT ETL Main Thread Finished.")

