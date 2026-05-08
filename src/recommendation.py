
import os
from pyspark.ml.recommendation import ALS
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, collect_set
from pyspark.sql.types import IntegerType

def create_recommendations_dir(output_dir):
    rec_dir = os.path.join(output_dir, "recommendations")
    if not os.path.exists(rec_dir):
        os.makedirs(rec_dir)
    return rec_dir

def train_collaborative_model(master_df, output_dir):
    """Collaborative Filtering (ALS) Model (Unchanged)"""
    print("\n--- Starting Collaborative Filtering (ALS) ---")
    rec_dir = create_recommendations_dir(output_dir)
    
    model_df = master_df.dropna(subset=["review_score", "customer_id", "product_id"])
    
    print("Filtering malformed data and casting review_score...")
    model_df = model_df.filter(col("review_score").rlike("^[1-5]$"))
    model_df = model_df.withColumn("review_score", col("review_score").cast(IntegerType()))
    
    print("Indexing string IDs to numeric...")
    user_indexer = StringIndexer(inputCol="customer_id", outputCol="customer_numeric_id", handleInvalid="skip")
    item_indexer = StringIndexer(inputCol="product_id", outputCol="product_numeric_id", handleInvalid="skip")
    
    model_df = user_indexer.fit(model_df).transform(model_df)
    model_df = item_indexer.fit(model_df).transform(model_df)
    
    (training, test) = model_df.randomSplit([0.8, 0.2], seed=42)
    
    print("Training ALS Model...")
    als = ALS(maxIter=5, regParam=0.1, userCol="customer_numeric_id", 
              itemCol="product_numeric_id", ratingCol="review_score",
              coldStartStrategy="drop") 
    model = als.fit(training)
    
    print("Evaluating ALS Model...")
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="review_score", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print(f"ALS Root-mean-square error (RMSE) = {rmse:.4f}")
    
    print("Generating top 5 recommendations per user...")
    user_recs = model.recommendForAllUsers(5)
    
    sample_recs = user_recs.limit(100).toPandas()
    als_path = os.path.join(rec_dir, "als_recommendations_sample.csv")
    sample_recs.to_csv(als_path, index=False)
    print(f"ALS recommendations saved to: {als_path}")


# --- NEW: Added products_df to the function arguments ---
def train_association_model(items_df, products_df, output_dir):
    """
    Builds an Association Rule model using FP-Growth based on Product Categories.
    """
    print("\n--- Starting Category Association Rules (FP-Growth) ---")
    rec_dir = create_recommendations_dir(output_dir)
    
    # 1. Join items with products to get category names
    print("Joining items with product categories...")
    products_clean = products_df.dropna(subset=["product_category_name"])
    joined_df = items_df.join(products_clean, "product_id", "inner")
    
    # 2. Prepare Data: Group by order_id, collecting categories instead of IDs
    print("Grouping categories by order basket...")
    baskets_df = joined_df.groupBy("order_id").agg(collect_set("product_category_name").alias("items"))
    
    # 3. Train the FP-Growth Model 
    # Since categories are much broader, we can use a healthy Support threshold
    print("Training FP-Growth Model on Categories...")
    fpGrowth = FPGrowth(itemsCol="items", minSupport=0.0001, minConfidence=0.01)
    model = fpGrowth.fit(baskets_df)
    
    # 4. Generate Rules
    print("Generating Category Association Rules...")
    rules = model.associationRules
    
    # 5. Save the rules
    rules_pandas = rules.orderBy(col("lift").desc()).limit(100).toPandas()
    fp_path = os.path.join(rec_dir, "fp_growth_category_rules.csv")
    rules_pandas.to_csv(fp_path, index=False)
    
    print(f"FP-Growth category rules saved to: {fp_path}")
    print(f"Found {rules.count()} strong category associations.")