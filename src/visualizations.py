import os
import matplotlib.pyplot as plt
import seaborn as sns

def create_visualizations_dir(output_dir):
    """Ensures the visualizations directory exists."""
    vis_dir = os.path.join(output_dir, "visualizations")
    if not os.path.exists(vis_dir):
        os.makedirs(vis_dir)
    return vis_dir

def plot_delays_by_state(state_df, output_dir):
    """Creates a horizontal bar chart of the top 10 states with the worst delays."""
    print("Generating State Delays Chart...")
    vis_dir = create_visualizations_dir(output_dir)
    
    # Convert PySpark DF to Pandas
    pdf = state_df.limit(10).toPandas()
    
    # Set up the plot
    plt.figure(figsize=(10, 6))
    sns.barplot(x='avg_delay_days', y='customer_state', data=pdf, palette='Reds_r')
    
    plt.title('Top 10 Brazilian States by Average Delivery Delay', fontsize=14)
    plt.xlabel('Average Delay (Days) *Negative = Early*', fontsize=12)
    plt.ylabel('Customer State', fontsize=12)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    
    # Save the plot
    save_path = os.path.join(vis_dir, "state_delays_chart.png")
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()
    
    print(f"Chart saved to: {save_path}")

def plot_reviews_vs_delays(review_df, output_dir):
    """Creates a bar chart showing how delivery delays affect review scores."""
    print("Generating Review Impact Chart...")
    vis_dir = create_visualizations_dir(output_dir)
    
    # Convert PySpark DF to Pandas
    pdf = review_df.toPandas()
    
    # Set up the plot
    plt.figure(figsize=(8, 6))
    sns.barplot(x='review_score', y='avg_delivery_delay', data=pdf, palette='viridis')
    
    plt.title('Impact of Delivery Speed on Customer Review Scores', fontsize=14)
    plt.xlabel('Customer Review Score (1 to 5 Stars)', fontsize=12)
    plt.ylabel('Average Delivery Delay (Days)', fontsize=12)
    
    # Invert Y axis so "arriving early" (negative numbers) points upward for better visual context
    plt.gca().invert_yaxis() 
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Save the plot
    save_path = os.path.join(vis_dir, "reviews_vs_delays_chart.png")
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()
    
    print(f"Chart saved to: {save_path}")