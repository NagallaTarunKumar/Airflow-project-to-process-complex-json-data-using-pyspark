from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr
from pyspark import SparkContext
import configparser
import os
import logging

# Define the path to the config file in the 'configs' folder
config_file_path = os.path.join(os.path.dirname(__file__), '..', 'configs', 'configs.txt')

# Read the config file to get the paths
config = configparser.ConfigParser()
config.read(config_file_path)  # Path to your config file

# Directly store the paths in variables
source_path = config.get('source', 'path')
destination_path = config.get('destination', 'path')

default_args={
    "owner":"airflow",
    "retrie":2,
    "retry_delay": timedelta(minutes=2)
}


def transform_complex_data(**kwargs):
    
    # list files in the source directory
    raw_data_files = os.listdir(source_path)
        
    # Initialize list for new files to be processed
    new_files = []

    # Retrieve previously processed files from Airflow XComs
    processed_files = kwargs['ti'].xcom_pull(task_ids='store_processed_files') or []

    for file in raw_data_files:
        if file.endswith(".json") and file not in processed_files:
            new_files.append(file)
    
    # If there are new files to process
    processed_files += new_files    
    

    if new_files:
        try:
            for file in new_files:
                # Define spark session
                spark = SparkSession.builder\
                .master("yarn")\
                .appName("Airflow_project")\
                .master("local[*]")\
                .getOrCreate()

                # Read data from the source
                df = spark.read\
                .format("json")\
                .option("multiline","true")\
                .option("header","true")\
                .option("inferschema","true")\
                .load(os.path.join(raw_data_files,file))

                #Process the data
                processed_df = df.withColumn("accounting",explode(col("accounting")))\
                    .withColumn("A_age", expr("accounting.age"))\
                    .withColumn("A_firstName",expr("accounting.firstName"))\
                    .withColumn("A_lastName", expr("accounting.lastName"))\
                    .withColumn("id",expr("id"))\
                    .withColumn("sales",explode(col("sales")))\
                    .withColumn("S_age",expr("sales.age"))\
                    .withColumn("S_firstName",expr("sales.firstName"))\
                    .withColumn("S_lastName",expr("sales.lastName"))\
                    .drop("accounting","sales")
                     

                present_day = datetime.now()
                present_day_date = present_day.strftime("%Y-%m-%d")
               
                # Define the output path for saving the CSV
                output_dir = os.path.join(os.path.dirname(__file__), '..', 'processed_data')

                os.makedirs(output_dir, exist_ok=True)  # Create the output directory if it doesn't exist

                # Define the output CSV file path with previous day's date
                output_file_path = os.path.join(output_dir, f"{present_day_date}")

                # Write the processed DataFrame to CSV
                processed_df.coalesce(1).write\
                .mode('append')\
                .option("header", "true")\
                .csv(output_file_path)

                logging.info(f"Processed data written to: {output_file_path}")

        except Exception as e:
            logging.error(f"Error processing {file}: {str(e)}")

    # Push the list of processed files to XComs
    kwargs['ti'].xcom_push(key='processed_files', value=processed_files)

def store_processed_files(**kwargs):
    """Store the list of processed files in XComs."""
    # This will be called after the process_files task to store the processed files in XComs.
    processed_files = kwargs['ti'].xcom_pull(task_ids='process_files', key='processed_files') or []
    logging.info(f"Processed files retrieved from XComs: {processed_files}")
    return processed_files


with DAG(
    default_args = default_args,
    dag_id = "First_dag_03",
    description = "first practice dag",
    start_date = datetime(2024,11,1),
    schedule= '0 1 * * *'

 ) as dag:
    transform_data = PythonOperator(
    task_id= "read_data",
    python_callable = transform_complex_data,
       
    )
    store_processed_files_to_xcoms = PythonOperator(
        task_id="store_processed_files_to_xcoms",
        python_callable=store_processed_files
    )



    transform_data >> store_processed_files_to_xcoms