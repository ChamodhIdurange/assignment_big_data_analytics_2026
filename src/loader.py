"""
loader.py
Functions to load raw CSV data into PySpark DataFrames.
"""
# --- NEW: Imported PRODUCTS_PATH ---
from config import ORDERS_PATH, CUSTOMERS_PATH, ITEMS_PATH, REVIEWS_PATH, PRODUCTS_PATH

def load_datasets(spark):
    """
    Loads the core Olist datasets into PySpark DataFrames.
    Returns a dictionary containing the DataFrames.
    """
    print("Loading datasets from /data/raw/...")
    
    try:
        orders_df = spark.read.csv(ORDERS_PATH, header=True, inferSchema=True)
        customers_df = spark.read.csv(CUSTOMERS_PATH, header=True, inferSchema=True)
        items_df = spark.read.csv(ITEMS_PATH, header=True, inferSchema=True)
        
        reviews_df = spark.read.csv(
            REVIEWS_PATH, 
            header=True, 
            inferSchema=True, 
            multiLine=True, 
            escape='"'
        )
        
        # --- NEW: Load Products Data ---
        products_df = spark.read.csv(PRODUCTS_PATH, header=True, inferSchema=True)
        
        print("Datasets loaded successfully.")
        
        return {
            "orders": orders_df,
            "customers": customers_df,
            "items": items_df,
            "reviews": reviews_df,
            "products": products_df # Added to dictionary
        }
        
    except Exception as e:
        print(f"Error loading datasets. Ensure all CSV files are in 'data/raw'. Error: {e}")
        return None