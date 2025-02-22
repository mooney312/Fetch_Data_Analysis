from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, StringType
from pyspark.sql.functions import col, from_json, trim, when, explode, explode_outer, regexp_replace
from pyspark.sql import functions as F

def load_json(file_path: str, spark_session: SparkSession, register_sql_view=False):
    pass
