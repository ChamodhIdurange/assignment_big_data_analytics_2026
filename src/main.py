"""
main.py
The main execution script that runs the entire PySpark pipeline (Analytics & ML).
"""
import os
import time

from config import get_spark_session, OUTPUT_DIR
from loader import load_datasets
from preprocessor import clean_orders_data, build_master_dataframe
from analytics import engineer_delivery_features, analyze_delays_by_state, analyze_reviews_vs_delays
from visualizations import plot_delays_by_state, plot_reviews_vs_delays
from recommendation import train_collaborative_model, train_association_model

def ensure_output_dirs_exist():
    data_out = os.path.join(OUTPUT_DIR, "data")
    if not os.path.exists(data_out):
        os.makedirs(data_out)
    return data_out

def main():
    start_time = time.time()
    print("==================================================")
    print("Starting Olist Big Data Pipeline (Parts A & B)")
    print("==================================================")
    
    out_dir = ensure_output_dirs_exist()
    spark = get_spark_session()
    
    datasets = load_datasets(spark)
    if not datasets:
        print("Pipeline aborted due to missing data.")
        return

    cleaned_orders = clean_orders_data(datasets["orders"])
    master_df = build_master_dataframe(
        cleaned_orders, 
        datasets["customers"], 
        datasets["items"], 
        datasets["reviews"]
    )
    
    # ---------------------------------------------------------
    # PART A: ANALYTICS
    # ---------------------------------------------------------
    analytics_df = engineer_delivery_features(master_df)
    analytics_df.cache() 
    
    state_df = analyze_delays_by_state(analytics_df, out_dir)
    state_df.show(5) 
    
    review_df = analyze_reviews_vs_delays(analytics_df, out_dir)
    review_df.show() 
    
    print("\n--- Generating Visualizations ---")
    plot_delays_by_state(state_df, OUTPUT_DIR)
    plot_reviews_vs_delays(review_df, OUTPUT_DIR)

    # ---------------------------------------------------------
    # PART B: RECOMMENDATION SYSTEMS
    # ---------------------------------------------------------
    # 1. Collaborative Filtering (ALS)
    train_collaborative_model(master_df, OUTPUT_DIR)
    
    # 2. Association Rules (FP-Growth) - NOW USING CATEGORIES
    # --- NEW: Passing datasets["products"] ---
    train_association_model(datasets["items"], datasets["products"], OUTPUT_DIR)
    
    spark.stop()
    
    end_time = time.time()
    print("==================================================")
    print(f"Pipeline completed successfully in {round(end_time - start_time, 2)} seconds.")
    print("==================================================")

if __name__ == "__main__":
    main()