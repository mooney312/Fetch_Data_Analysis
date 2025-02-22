import pandas as pd
from pyspark.sql import SparkSession
from src.data_ingestion import load_json
from fetch_project.src.data_transform import transform_users

def main():
    """Main function to orchestrate the data processing workflow."""
    
    # Step 1: Initialize Spark Session
    spark = SparkSession.builder.master("local").appName("FetchDataProcessing").getOrCreate()
    
    pass

    # Optional: Stop Spark session (only if running standalone)
    spark.stop()

if __name__ == "__main__":
    main()
