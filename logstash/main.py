import datetime
import glob
import os
import pytz
import requests
import shutil
import threading
import zipfile
from elasticsearch import Elasticsearch, NotFoundError
from io import BytesIO
from time import sleep
from typing import List
import json
from dotenv import load_dotenv

# PySpark imports
from pyspark.sql.functions import col, struct, array_distinct, concat_ws
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Local imports
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser


load_dotenv()


################################################## Constants ##################################################
USER = os.getenv("ELASTIC_USER")
PASSWORD = os.getenv("ELASTIC_PASSWORD")
LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
DOWNLOAD_FOLDER = "./csv"
LOG_FILE = "./logs/log.txt"
SCRAPING_LOG_FILE = "./logs/scraping_log.txt"
INGESTION_LOG_FILE = "./logs/ingestion_log.txt"
TIMESTAMP_LOG_FILE = "./logs/timestamp_log.txt"
JSON_LOG_FILE = "./logs/json_log.txt"
LOGSTASH_PATH = "./logstash_ingest_data/json"
PYSPARK_LOG_FILE = "./logs/pyspark_log.txt"

if None in (USER, PASSWORD):
    raise Exception("Environment variable cannot be None!")

################################################ DIR handling #################################################
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs("./logs", exist_ok=True)
os.makedirs(LOGSTASH_PATH, exist_ok=True)
for file in [LOG_FILE, SCRAPING_LOG_FILE, INGESTION_LOG_FILE, TIMESTAMP_LOG_FILE, JSON_LOG_FILE, PYSPARK_LOG_FILE]:
    with open(file, "w") as f:
        f.write("")


################################################## Functions ##################################################
def write(message: str, file: str) -> None:
    '''
    Writes message to the file in the specified file path.
    Adds a current timestamp to the content string before appending it
    to the back of the file.

    Args:
        message (str): String to append to end of log file.
        file (str): The file path to the required log file.
    '''
    timezone = pytz.timezone("Asia/Singapore")
    current_time_gmt8 = datetime.datetime.now(timezone)
    current_time = current_time_gmt8.strftime("%Y-%m-%d %H:%M:%S") + ": "    
    with open(file, "a", encoding="utf-8") as f:
        f.write(current_time + message + "\n")


def write_all(message: str, file_list: list[str] = [LOG_FILE, INGESTION_LOG_FILE, JSON_LOG_FILE]) -> None:
    '''
    Writes the message to all files in file_list,
    using the write(message, file) function.

    Args:
        message (str): String to append to end of log file.
        file_list (List[str], optional): List of file paths to write message into. 
    '''
    for file in file_list:
        write(message, file)


def run_web_query(url: str) -> requests.Response:
    '''
    Fetches content from the URL.
    If URL is unreachable, repeats the process again after 5s
    until a response is received.

    Args:
        url (str): URL to fetch data from.

    Returns:
        requests.Response: The raw HTTP response object returned by `requests.get()`.
    '''
    response = requests.get(url)
    if response.status_code != 200:
        write_all(f"Error fetching {LAST_UPDATE_URL}; trying again...", [LOG_FILE, SCRAPING_LOG_FILE])
        sleep(5)
        return run_web_query(url)
    return response


def get_latest_gdelt_links() -> list[str]:
    """
    Fetches the latest update file and extracts the download URLs.

    Returns:
        list[str]: The list of URLs fetched from the last updated text file in GDELT.
    """
    response = requests.get(LAST_UPDATE_URL)

    if response.status_code != 200:
        write(message="Failed to fetch lastupdate.txt.", file=SCRAPING_LOG_FILE)
        return []

    lines = response.text.strip().split("\n")
    urls = [line.split()[-1] for line in lines if line.endswith(".zip")]
    return urls


def download_and_extract(url: str) -> None:
    """
    Downloads a ZIP file from the given URL and extracts CSV files.
    Checks against existing files to avoid repeat extraction.

    Args:
        url (str): The URL to download the zip file from.
    """
    file_name = url.split("/")[-1]
    if os.path.exists(os.path.join(DOWNLOAD_FOLDER, file_name.replace(".zip", ""))):
         write_all(f"Extraction skipped: {file_name} (or contents) already exists.", [LOG_FILE, SCRAPING_LOG_FILE])
         return

    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        write(f"Failed to get {url}", SCRAPING_LOG_FILE)
        return
    
    try:
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        for file in zip_file.namelist():
            if file not in os.listdir(DOWNLOAD_FOLDER):
                if file.lower().endswith("gkg.csv"):
                    write_all(f"Extracting latest file (15 min interval): {file}", [LOG_FILE, SCRAPING_LOG_FILE])
                    zip_file.extract(file, DOWNLOAD_FOLDER)
                    write_all(f"Extracted latest file (15 min interval): {file}", [LOG_FILE, SCRAPING_LOG_FILE])
            else: 
                write_all(f"Extraction skipped: {file} already exists.", [LOG_FILE, SCRAPING_LOG_FILE])
    except zipfile.BadZipFile:
        write(f"Error: Bad Zip File for {url}", SCRAPING_LOG_FILE)


def restructure_columns(df: DataFrame, column_name: str, fields: List[str]) -> DataFrame:
    '''
    Restructures the dataframe column with its desired fields accordingly.

    Args:
        df (DataFrame): Spark dataframe to be transformed.
        column_name (str): The name of the column to transform.
        fields (List[str]): All fields to be included under the column, eg. Tone, Polarity etc.
                            Fields will be edited by function to take on the format
                            `[column_name].[field]` automatically.

    Returns:
        DataFrame: PySpark dataframe containing the transformed column.
    '''
    struct_fields = [col(f"{column_name}.{field}") for field in fields]
    return df.withColumn(column_name, struct(*struct_fields))


def restructure_array_struct_column(df: DataFrame, column_name: str, fields: list[str]) -> DataFrame:
    """
    Creates or replaces a struct column composed of array_distinct-applied fields.

    Args:
        df (DataFrame): The input Spark DataFrame.
        column_name (str): The name of the new structured column.
        fields (List[str]): List of full field paths (e.g., 'V2Locations.FullName').

    Returns:
        DataFrame: Updated DataFrame with the new structured column.
    """
    struct_fields = [array_distinct(col(f"{column_name}.{field}")).alias(field) for field in fields]
    return df.withColumn(column_name, struct(*struct_fields))


def run_pipeline(raw_file: str, json_output: str) -> None:
    """
    Reads a raw GKG CSV file, transforms each line using gkg_parser,
    creates a Spark DataFrame with the defined schema,
    and writes the output as a single JSON file.
    The JSON file is saved in the LOGSTASH_PATH volume.

    Args:
        raw_file (str): Directory containing full file path of raw CSV file.
        json_output (str): Path where JSON files should be output into.
    """
    spark = SparkSession.builder.appName("Standalone GKG ETL").getOrCreate()
    rdd = spark.sparkContext.textFile(raw_file)    
    parsed_rdd = rdd.map(lambda line: gkg_parser(line))
    df = spark.createDataFrame(parsed_rdd, schema=gkg_schema)
    df_transformed = df.withColumn(
        "RecordId",
        concat_ws("-", col("GkgRecordId.Date").cast("string"), col("GkgRecordId.NumberInBatch").cast("string"))
    )

    to_drop = ["V1Counts", "V1Locations", "V1Orgs", "V1Persons", "V1Themes", "V21Amounts", "V21Counts", "V21EnhancedDates"]
    for i in to_drop:
        df_transformed = df_transformed.drop(i)

    V15Tone_fields = ['Tone', 'PositiveScore', "NegativeScore", 'Polarity', "ActivityRefDensity", "SelfGroupRefDensity"]
    V21Quotations_fields = ["Verb", "Quote"]
    V2Persons_fields = ["V1Person"]
    V2Orgs_fields = ["V1Org"]
    V2Locations_fields = ["FullName", "CountryCode", "ADM1Code", "ADM2Code", "LocationLatitude", "LocationLongitude", "FeatureId"]
    V2EnhancedThemes_fields = ["V2Theme"]
    V2GCAM_fields = ["DictionaryDimId"]
    V21AllNames_fields = ["Name"]

    df_transformed = restructure_columns(df_transformed, "V15Tone", V15Tone_fields)
    df_transformed = restructure_array_struct_column(df_transformed, "V2Locations", V2Locations_fields)
    df_transformed = restructure_array_struct_column(df_transformed, "V2Persons", V2Persons_fields)
    df_transformed = restructure_array_struct_column(df_transformed, "V2EnhancedThemes", V2EnhancedThemes_fields)
    df_transformed = restructure_array_struct_column(df_transformed, "V2Orgs", V2Orgs_fields)
    df_transformed = restructure_array_struct_column(df_transformed, "V2GCAM", V2GCAM_fields)
    df_transformed = restructure_array_struct_column(df_transformed, "V21Quotations", V21Quotations_fields)
    df_transformed = restructure_array_struct_column(df_transformed, "V21AllNames", V21AllNames_fields)
    
    column_names = ["V21ShareImg", "V21SocImage", "V2DocId", "V21RelImg", "V21Date"]
    for col_name in column_names:
        df_transformed = df_transformed.withColumn(col_name, col(f"{col_name}.{col_name}"))

    df_transformed = df_transformed.withColumn("datatype", F.lit("gkg"))
    df_transformed.coalesce(1).write.mode("overwrite").json(json_output)
    write_all(f"Pipeline completed. Single JSON output written to {json_output}", [LOG_FILE, JSON_LOG_FILE])
    json_part_file = glob.glob(os.path.join(json_output, "part-00000-*.json"))[0]
    date_part = str((raw_file.split('/')[2].split('.'))[0])
    new_file_name = f"{date_part}.json"
    shutil.move(json_part_file, os.path.join(json_output, new_file_name))
    move_json_to_ingest(os.path.join(json_output, new_file_name))
    spark.stop()


def move_json_to_ingest(file_path: str) -> None:
    '''
    Moves the JSON file over to the json subfolder in logstash_ingest_data.
    
    Args:
        file_path (str): File path to write the JSON file into.
    '''
    target_path = os.path.join(LOGSTASH_PATH, os.path.basename(file_path))
    shutil.move(file_path, target_path)
    write_all(f"Copied {file_path} to {target_path}", [LOG_FILE, JSON_LOG_FILE])


def es_client_setup() -> Elasticsearch:
    """
    Sets up a secure instance of Elasticsearch client with SSL verification.

    Returns:
        Elasticsearch: The Elasticsearch client to send queries to.
    """
    es_client = Elasticsearch(
        "https://es01:9200",
        basic_auth=(USER, PASSWORD),
        ca_certs="/usr/share/logstash/certs/ca/ca.crt", 
        verify_certs=True,
        ssl_show_warn=True, 
        request_timeout=30
    )
    return es_client


def es_check_data(timestamp_str: str) -> bool:
    '''
    Queries Elasticsearch to check if data for given timestamp exists.

    Args:
        timestamp_str (str): String containing the timestamp of the file.

    Returns:
        bool: True if a file with such a timestamp exists in Elasticsearch, and False otherwise.
    '''
    client = es_client_setup()
    query_body = {"query": {"term": {"GkgRecordId.Date": timestamp_str}}}
    response = client.count(index='gkg*', body=query_body, request_timeout=10)
    return response.get('count', 0) > 0


def process_downloaded_files() -> None:
    '''
    Infinite looping function that process downloaded CSV files via PySpark dataframe,
    converts them into JSON format, and finally deletes the relevant files / folders.
    '''
    src_path = DOWNLOAD_FOLDER
    
    while True:
        try:
            files = os.listdir(src_path)
            csv_files = [f for f in files if f.endswith(".csv")]
            
            if not csv_files: 
                sleep(5)
                continue
                
            file = csv_files[0]
            raw_file_path = os.path.join(src_path, file)
            json_output_path = raw_file_path.replace(".csv", ".json")
            
            json_file_name = file.replace(".gkg.csv", ".json")
            timestamp_str = json_file_name.split(".")[0]
            
            if es_check_data(timestamp_str):
                write_all(f"Transformation skipped: {json_file_name} already exists inside Elasticsearch")
                if os.path.exists(raw_file_path):
                    os.remove(raw_file_path)
                continue
            
            with open(PYSPARK_LOG_FILE, "w") as f:
                f.write(timestamp_str)
            
            write_all(f"Transforming file into JSON: {file}")
            try:
                run_pipeline(raw_file_path, json_output_path)
            except Exception as e:
                write_all(f"Error during pipeline execution for {file}: {e}", [LOG_FILE, JSON_LOG_FILE])

                with open(PYSPARK_LOG_FILE, "w") as f:
                    f.write("")
                continue

            with open(PYSPARK_LOG_FILE, "w") as f:
                f.write("")

            sleep(1)
            
            if os.path.exists(raw_file_path):
                os.remove(raw_file_path)
                write_all(f"Deleted processed CSV file: {raw_file_path}", [LOG_FILE, JSON_LOG_FILE])

            json_folder = file.split(".")[0] + ".gkg.json"
            json_folder_full = os.path.join(src_path, json_folder)

            if os.path.exists(json_folder_full):
                try:
                    shutil.rmtree(json_folder_full)
                except Exception as e:
                    write_all(f"Error deleting Spark folder {json_folder}: {e}", [LOG_FILE, JSON_LOG_FILE])

        except Exception as e:
            write_all(f"Critical error in process loop: {e}", [LOG_FILE])
            sleep(10)


def delete_processed_json() -> None:
    '''
    Checks JSON folder constantly, & deletes JSON files already ingested into Elasticsearch.
    '''
    directory = LOGSTASH_PATH
    
    while True:
        sleep(10)
        try:
            all_json = [i for i in os.listdir(directory) if ".json" in i]
            for filename in all_json:
                # Check if this specific timestamp exists in ES
                if es_check_data(filename.split(".")[0]):
                    file_path = os.path.join(directory, filename)

                    # Stop deletion if Spark is currently writing this specific timestamp
                    # (Prevents race condition where ES sees old data but Spark is writing new data)
                    try:
                        with open(PYSPARK_LOG_FILE, "r") as f:
                            timestamp = f.read().strip()

                        if timestamp and timestamp in file_path:
                            continue

                    except FileNotFoundError:
                        pass

                    try:
                        os.remove(file_path)
                        write_all(f"Cleaned up ingested JSON file: {filename}", [LOG_FILE, JSON_LOG_FILE])

                    except OSError as e:
                        write_all(f"Error deleting {filename}: {e}", [LOG_FILE])

        except Exception as e:
            write_all(f"Error in delete thread: {e}", [LOG_FILE])


def setup_elasticsearch(policy_name: str, policy_file: str, template_name: str, template_file: str) -> None:
    """
    Connects to Elasticsearch to set up the ILM policy and index template idempotently.

    Args:
        policy_name (str): The name for the ILM policy.
        policy_file (str): The file path to the ILM policy JSON file.
        template_name (str): The name for the index template.
        template_file (str): The file path to the index template JSON file.
    """
    try:
        es = es_client_setup()
        
        # 1. Setup ILM Policy
        with open(policy_file, "r") as f:
            policy_body = json.load(f)
        
        try:
            existing_policy = es.ilm.get_lifecycle(name=policy_name)
            if existing_policy[policy_name]["policy"] != policy_body["policy"]:
                es.ilm.put_lifecycle(name=policy_name, body=policy_body)
                write(f"Successfully updated ILM policy: {policy_name}", LOG_FILE)
            else:
                write(f"ILM policy '{policy_name}' is already up to date.", LOG_FILE)
        except NotFoundError:
            es.ilm.put_lifecycle(name=policy_name, body=policy_body)
            write(f"Successfully created ILM policy: {policy_name}", LOG_FILE)

        # 2. Setup Index Template
        with open(template_file, "r") as f:
            template_body = json.load(f)

        try:
            existing_template = es.indices.get_index_template(name=template_name)
            if existing_template["index_templates"][0]["index_template"] != template_body:
                 es.indices.put_index_template(name=template_name, body=template_body)
                 write(f"Successfully updated index template: {template_name}", LOG_FILE)
            else:
                write(f"Index template '{template_name}' is already up to date.", LOG_FILE)
        except NotFoundError:
            es.indices.put_index_template(name=template_name, body=template_body)
            write(f"Successfully created index template: {template_name}", LOG_FILE)

    except Exception as e:
        write(f"Error setting up Elasticsearch: {e}", LOG_FILE)
    

def server_scrape():
    '''
    Scrapes data off the GDELT server,
    and downloads the resultant CSV files every 15 minutes.
    '''
    file_list = [LOG_FILE, SCRAPING_LOG_FILE]
    while True:
        try:
            csv_zip_urls = get_latest_gdelt_links()

            if not csv_zip_urls:
                write_all("No CSV ZIP links found in lastupdate.txt", file_list)
            else:
                write_all(f"Found {len(csv_zip_urls)} files to download (15 min interval)...", file_list)
                for url in csv_zip_urls:
                    download_and_extract(url)

            write("\n", TIMESTAMP_LOG_FILE)

            # Repeats the scraping and downloading process every 15 min
            sleep(15*60)     

        except Exception as e:
            write_all(f"An error occurred during the scraping process: {e}", file_list)
            sleep(60)  # Add a delay to prevent rapid retries on persistent errors.


############################################# Main ############################################
if __name__ == "__main__":
    setup_elasticsearch(
        policy_name="gdelt_14d_retention_policy",
        policy_file="ilm_policy.json",
        template_name="gkg-template",
        template_file="template.json"
    )

    thread1 = threading.Thread(target=server_scrape, daemon=True)
    thread2 = threading.Thread(target=process_downloaded_files, daemon=True)
    thread3 = threading.Thread(target=delete_processed_json, daemon=True)
    
    thread1.start()
    thread2.start()
    thread3.start()
    
    thread1.join()
    thread2.join()
    thread3.join()