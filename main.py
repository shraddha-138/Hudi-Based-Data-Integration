import argparse
import os
from pyspark.sql import SparkSession
from fileprocessor import FileProcessor
from acquirer import AcquirerProcessor   
from gateway import GatewayProcessor 
from config import hudi_path 

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process Gateway or Acquirer files and validate Hudi ingestion.")
    parser.add_argument("--file", type=str, required=True, help="Path to the input file (Gateway or Acquirer).")

    args = parser.parse_args()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("HudiTableProcessing") \
        .config("spark.jars", "/home/shraddha/Downloads/hudi-spark3.5-bundle_2.12-0.15.0.jar") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.14.0") \
        .getOrCreate()

    try:
        # Get the file extension
        file_extension = os.path.splitext(args.file)[1].lower()

        # Instantiate the appropriate processor based on the file extension
        if file_extension in ['.xls', '.xlsx']:
            processor = AcquirerProcessor(spark)  # Instantiate AcquirerProcessor for Excel files
            processor.process_acquirer_file(args.file)  

        elif file_extension in ['.txt', '.csv']:
            processor = GatewayProcessor(spark)  # Instantiate GatewayProcessor for text and CSV files
            processor.process_gateway_file(args.file)  

        else:
            print("Unsupported file type. Please provide a file with .xls, .xlsx, .txt, or .csv extensions.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    main()
