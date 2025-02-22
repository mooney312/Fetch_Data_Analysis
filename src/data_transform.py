from pyspark.sql import SparkSession
from src.data_ingestion import load_csv, load_invoices

def transform_users(spark: SparkSession):
    """
    Transforms the orders dataset and creates a SQL view for downstream usage.
    :param spark: SparkSession object

    """
    pass