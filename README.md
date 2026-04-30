# Olist Big Data Analytics Pipeline (Part A)

## Overview
This project analyzes the Brazilian E-Commerce Public Dataset by Olist using Apache Spark (PySpark). The objective of this analytics pipeline is to determine how delivery logistics (specifically delivery delays) impact customer satisfaction (review scores) across different Brazilian states.

## Prerequisites
To run this pipeline, you must have Python installed along with the following libraries:
- `pyspark`
- `pandas`
- `matplotlib`
- `seaborn`

You can install these dependencies by running: 
`pip install pyspark pandas matplotlib seaborn`

## Project Structure
- `data/raw/`: Place the 4 required Olist CSV files here (`olist_orders_dataset.csv`, `olist_customers_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_reviews_dataset.csv`).
- `src/`: Contains all modular PySpark scripts (`config.py`, `loader.py`, `preprocessor.py`, `analytics.py`, `visualizations.py`, `main.py`).
- `outputs/`: The script will automatically generate CSV summaries and PNG charts in this folder.

## Execution Instructions
1. Ensure the Olist CSV files are placed in the `data/raw/` directory.
2. Open your terminal and navigate to the root directory of this project.
3. Run the main pipeline script using the following command:
   `python src/main.py`
4. The script will execute the PySpark data engineering pipeline and display interactive charts on your screen. **You must close the first chart window for the script to continue to the second chart.**
5. Final summary CSVs and high-resolution chart images will be saved automatically in the `outputs/` folder.