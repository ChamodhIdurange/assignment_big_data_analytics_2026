
from pyspark.sql.functions import col, datediff, avg, desc, count
import os

def engineer_delivery_features(master_df):
    # estimated delivery date and the actual delivery date.
    print("Engineering delivery delay features...")
    
    # Calculate delay: Positive number = Late, Negative number = Early
    analytics_df = master_df.withColumn(
        "delivery_delay_days", 
        datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))
    )
    return analytics_df


def analyze_delays_by_state(analytics_df, output_dir):
    # Calculates the average delivery delay per Brazilian state and saves the result.
    print("Analyzing average delivery delays by customer state...")
    
    state_delays = analytics_df.groupBy("customer_state") \
        .agg(
            avg("delivery_delay_days").alias("avg_delay_days"),
            count("order_id").alias("total_orders")
        ) \
        .filter(col("total_orders") > 100) \
        .orderBy(desc("avg_delay_days"))
    
    # Convert to Pandas to save as a small, readable CSV for presentation charts
    pandas_df = state_delays.toPandas()
    output_path = os.path.join(output_dir, "state_delays_summary.csv")
    pandas_df.to_csv(output_path, index=False)
    
    print(f"State delays analysis saved to: {output_path}")
    return state_delays


def analyze_reviews_vs_delays(analytics_df, output_dir):
    # Analyzes how the delivery delay impacts the 1 to 5 star customer review score.
    print("Analyzing correlation between delays and review scores...")
    
    # Filter out orders without reviews for this specific analysis
    reviews_df = analytics_df.dropna(subset=["review_score"])
    
    impact_analysis = reviews_df.groupBy("review_score") \
        .agg(
            avg("delivery_delay_days").alias("avg_delivery_delay"),
            count("order_id").alias("review_count")
        ) \
        .orderBy("review_score")
        
    # Convert to Pandas and save
    pandas_df = impact_analysis.toPandas()
    output_path = os.path.join(output_dir, "reviews_vs_delays.csv")
    pandas_df.to_csv(output_path, index=False)
    
    print(f"Review impact analysis saved to: {output_path}")
    return impact_analysis