"""
loader.py
Functions to load raw CSV data into PySpark DataFrames.
"""
from config import ORDERS_PATH, CUSTOMERS_PATH, ITEMS_PATH, REVIEWS_PATH

def load_datasets(spark):
    """
    Loads the four core Olist datasets into PySpark DataFrames.
    Returns a dictionary containing the DataFrames.
    """
    print("Loading datasets from /data/raw/...")
    
    try:
        # Read CSV files, inferring schema automatically
        orders_df = spark.read.csv(ORDERS_PATH, header=True, inferSchema=True)
        customers_df = spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True)
        items_df = spark.read.csv(ITEMS_PATH, header=True, inferSchema=True)
        
        # FIX: Added multiLine and escape to properly parse messy text reviews
        reviews_df = spark.read.csv(
            REVIEWS_PATH, 
            header=True, 
            inferSchema=True, 
            multiLine=True, 
            escape='"'
        )
        
        print("Datasets loaded successfully.")
        
        return {
            "orders": orders_df,
            "customers": customers_df,
            "items": items_df,
            "reviews": reviews_df
        }
        
    except Exception as e:
        print(f"Error loading datasets. Ensure CSV files are in the 'data/raw' folder. Error: {e}")
        return None