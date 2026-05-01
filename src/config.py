"""
config.py
Configuration settings and Spark session initialization.
"""
from pyspark.sql import SparkSession
import os

# Define absolute paths based on the project structure
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")

# File paths for the specific Olist datasets
ORDERS_PATH = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
CUSTOMERS_PATH = os.path.join(DATA_DIR, "olist_customers_dataset.csv")
ITEMS_PATH = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")
REVIEWS_PATH = os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv")
# --- NEW: Added Products Path ---
PRODUCTS_PATH = os.path.join(DATA_DIR, "olist_products_dataset.csv")

def get_spark_session(app_name="Olist_Logistics_Analytics"):
    """
    Creates and returns a PySpark session.
    """
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR") 
    return spark