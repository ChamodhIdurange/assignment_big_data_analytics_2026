"""
recommendation.py
Contains Spark MLlib models for Collaborative Filtering (ALS) and Association Rules (FP-Growth).
"""
import os
from pyspark.ml.recommendation import ALS
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
# FIX: Switched collect_list to collect_set to prevent duplicate item errors
from pyspark.sql.functions import col, collect_set 
from pyspark.sql.types import IntegerType

def create_recommendations_dir(output_dir):
    """Ensures the recommendations output directory exists."""
    rec_dir = os.path.join(output_dir, "recommendations")
    if not os.path.exists(rec_dir):
        os.makedirs(rec_dir)
    return rec_dir

def train_collaborative_model(master_df, output_dir):
    """
    Builds a Collaborative Filtering model using ALS to recommend products 
    based on past user review scores.
    """
    print("\n--- Starting Collaborative Filtering (ALS) ---")
    rec_dir = create_recommendations_dir(output_dir)
    
    # 1. Prepare Data: Drop empty rows
    model_df = master_df.dropna(subset=["review_score", "customer_id", "product_id"])
    
    print("Filtering malformed data and casting review_score...")
    model_df = model_df.filter(col("review_score").rlike("^[1-5]$"))
    model_df = model_df.withColumn("review_score", col("review_score").cast(IntegerType()))
    
    # 2. Convert String IDs to Numeric IDs (ALS requires integers)
    print("Indexing string IDs to numeric...")
    user_indexer = StringIndexer(inputCol="customer_id", outputCol="customer_numeric_id", handleInvalid="skip")
    item_indexer = StringIndexer(inputCol="product_id", outputCol="product_numeric_id", handleInvalid="skip")
    
    model_df = user_indexer.fit(model_df).transform(model_df)
    model_df = item_indexer.fit(model_df).transform(model_df)
    
    # 3. Split Data into Training and Testing sets
    (training, test) = model_df.randomSplit([0.8, 0.2], seed=42)
    
    # 4. Train the ALS Model
    print("Training ALS Model...")
    als = ALS(maxIter=5, regParam=0.1, userCol="customer_numeric_id", 
              itemCol="product_numeric_id", ratingCol="review_score",
              coldStartStrategy="drop") 
    model = als.fit(training)
    
    # 5. Evaluate the Model
    print("Evaluating ALS Model...")
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="review_score", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print(f"ALS Root-mean-square error (RMSE) = {rmse:.4f}")
    
    # 6. Generate Recommendations
    print("Generating top 5 recommendations per user...")
    user_recs = model.recommendForAllUsers(5)
    
    sample_recs = user_recs.limit(100).toPandas()
    als_path = os.path.join(rec_dir, "als_recommendations_sample.csv")
    sample_recs.to_csv(als_path, index=False)
    print(f"ALS recommendations saved to: {als_path}")


def train_association_model(items_df, output_dir):
    """
    Builds an Association Rule model using FP-Growth to find items frequently 
    bought together in the same order (Market Basket Analysis).
    """
    print("\n--- Starting Association Rules (FP-Growth) ---")
    rec_dir = create_recommendations_dir(output_dir)
    
    # 1. Prepare Data: Group products into an array (basket) per order
    print("Grouping items by order...")
    # FIX: Using collect_set to ensure all items in a transaction are unique
    baskets_df = items_df.groupBy("order_id").agg(collect_set("product_id").alias("items"))
    
    # 2. Train the FP-Growth Model
    print("Training FP-Growth Model...")
    fpGrowth = FPGrowth(itemsCol="items", minSupport=0.001, minConfidence=0.01)
    model = fpGrowth.fit(baskets_df)
    
    # 3. Generate Rules & Evaluate
    print("Generating Association Rules...")
    rules = model.associationRules
    
    # 4. Save the rules
    rules_pandas = rules.orderBy(col("lift").desc()).limit(100).toPandas()
    fp_path = os.path.join(rec_dir, "fp_growth_rules.csv")
    rules_pandas.to_csv(fp_path, index=False)
    
    print(f"FP-Growth association rules saved to: {fp_path}")
    print(f"Found {rules.count()} strong product associations.")