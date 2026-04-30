import os
import time
from visualizations import plot_delays_by_state, plot_reviews_vs_delays

# Import our custom modules
from config import get_spark_session, OUTPUT_DIR
from loader import load_datasets
from preprocessor import clean_orders_data, build_master_dataframe
from analytics import engineer_delivery_features, analyze_delays_by_state, analyze_reviews_vs_delays

def ensure_output_dirs_exist():
    # Creates the output directories if they don't exist.
    data_out = os.path.join(OUTPUT_DIR, "data")
    if not os.path.exists(data_out):
        os.makedirs(data_out)
    return data_out

def main():
    start_time = time.time()
    print("Starting Olist Big Data Analytics Pipeline")
    # 0. Setup
    out_dir = ensure_output_dirs_exist()
    spark = get_spark_session()
    
    # 1. Load Data
    datasets = load_datasets(spark)
    if not datasets:
        print("Pipeline aborted due to missing data.")
        return

    # 2. Preprocess Data
    cleaned_orders = clean_orders_data(datasets["orders"])
    master_df = build_master_dataframe(
        cleaned_orders, 
        datasets["customers"], 
        datasets["items"], 
        datasets["reviews"]
    )
    
    # 3. Analytics & Feature Engineering
    analytics_df = engineer_delivery_features(master_df)
    
    # Cache the dataframe since we are using it for multiple aggregations
    analytics_df.cache()
    
    # 4. Generate Insights
    state_df = analyze_delays_by_state(analytics_df, out_dir)
    state_df.show(5) # Print top 5 to console for the demo video
    
    review_df = analyze_reviews_vs_delays(analytics_df, out_dir)
    review_df.show() # Print to console for demo video
    
    # 5. Teardown
    spark.stop()
    
    end_time = time.time()
    print(f"Pipeline completed successfully in {round(end_time - start_time, 2)} seconds.")

if __name__ == "__main__":
    main()