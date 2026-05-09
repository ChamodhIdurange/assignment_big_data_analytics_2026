# E-Commerce Big Data Analytics & Hybrid Recommendation System

## Project Overview

This project is an end-to-end Big Data analytics and machine learning pipeline built with Apache Spark (PySpark) and Python. It processes over 100,000 e-commerce orders to solve two core business challenges:

1. Part A (Analytics): Identifying regional logistics bottlenecks and mathematically proving how delivery speed drives customer satisfaction.
2. Part B (Recommendation System): Building a hybrid machine learning engine capable of both personalized user-to-item targeting (Collaborative Filtering) and real-time category cross-selling (Market Basket Analysis).

## Dataset Information

Data Source: This project utilizes an outsourced, real-world commercial dataset.

- Name: Brazilian E-Commerce Public Dataset by Olist
- Provider: Kaggle
- Link: [Download the Dataset Here](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

_Note: You must download the CSV files from the link above and place them in the `data/raw/` directory before running the pipeline._
_In old Pandas, people often save intermediate CSVs at every step. Because I built this in PySpark, the Master DataFrame is kept in-memory and cached. This will speed up the pipeline_

## 🛠️ Technical Architecture & Data Engineering

Building this pipeline required handling messy, real-world data across 5 distinct relational databases (Orders, Customers, Items, Reviews, Products).

- CSV Parsing Fixes: Addressed severe data corruption caused by carriage returns in customer text reviews by configuring the PySpark reader with `multiLine=True` and `escape='"'`.
- Data Sanitization: Utilized Regex filtering (`rlike("^[1-5]$")`) to isolate valid review scores before casting string IDs to numeric Integer types.
- Complex Joins: Executed high-performance Inner and Left Joins across all datasets to construct a unified Master DataFrame for analytics.
- Feature Engineering: Built a custom metric, `avg_delay_days`, by calculating the `datediff` between actual and estimated delivery timestamps.

## Technologies Used

- Apache Spark / PySpark (Distributed Data Processing & SQL)
- Spark MLlib (ALS & FP-Growth Machine Learning)
- Python (Core Programming)
- Pandas (Data Output & Formatting)
- Matplotlib & Seaborn (Data Visualization)

# How to Run the Project

1. Clone the repository.
2. Download the Olist dataset from the Kaggle link provided above and place the CSVs inside `data/raw/`.
3. Ensure your Python environment has PySpark installed (`pip install pyspark pandas matplotlib seaborn`).
4. Execute the main pipeline script: (`python3 src/main.py`)

   python src/main.py
