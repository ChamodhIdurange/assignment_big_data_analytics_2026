"""
preprocessor.py
Functions for cleaning data, handling dates, and joining tables.
"""
from pyspark.sql.functions import col, to_timestamp

def clean_orders_data(orders_df):
    """
    Converts string date columns to proper timestamps and filters 
    for successfully delivered orders.
    """
    print("Cleaning orders data...")
    
    # 1. Cast string dates to proper PySpark timestamps
    cleaned_df = orders_df \
        .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
        .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
        .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
    
    # 2. Filter to keep ONLY delivered orders (ignoring canceled/shipped statuses)
    delivered_df = cleaned_df.filter(col("order_status") == "delivered")
    
    # 3. Drop rows where actual delivery date is missing to avoid calculation errors
    final_orders = delivered_df.dropna(subset=["order_delivered_customer_date"])
    
    return final_orders


def build_master_dataframe(cleaned_orders, customers, items, reviews):
    """
    Joins the separate DataFrames into a single master DataFrame for analytics.
    """
    print("Building master analytics DataFrame via inner/left joins...")
    
    # Join Orders with Customers (to get location data like customer_state)
    df1 = cleaned_orders.join(customers, "customer_id", "inner")
    
    # Join with Items (to get order value and freight value)
    df2 = df1.join(items, "order_id", "inner")
    
    # Join with Reviews (to get 1 to 5 star customer satisfaction scores)
    # We drop duplicates on order_id to prevent data explosion if one order has multiple reviews
    reviews_unique = reviews.dropDuplicates(["order_id"])
    
    # We use a 'left' join here in case some orders never received a review
    master_df = df2.join(reviews_unique, "order_id", "left")
    
    print("Master DataFrame built successfully.")
    return master_df
