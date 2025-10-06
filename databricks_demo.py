# Databricks Demo Script
# 
# This script demonstrates basic Spark operations similar to Databricks notebooks.
# 
# Prerequisites:
# - PySpark installed: pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when

# =============================================================================
# 1. Initialize Spark Session
# =============================================================================

print("=" * 80)
print("1. INITIALIZE SPARK SESSION")
print("=" * 80)

# Create Spark session
spark = SparkSession.builder \
    .appName("Databricks Demo") \
    .master("local[*]") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print("✅ Spark session created successfully!")

# =============================================================================
# 2. Create Sample Data
# =============================================================================

print("\n" + "=" * 80)
print("2. CREATE SAMPLE DATA")
print("=" * 80)

# Sample sales data
data = [
    ("Alice", "Electronics", 1200, 5),
    ("Bob", "Clothing", 450, 3),
    ("Charlie", "Electronics", 800, 2),
    ("Alice", "Clothing", 300, 1),
    ("Bob", "Electronics", 950, 4),
    ("Charlie", "Clothing", 600, 2),
    ("Alice", "Electronics", 1500, 6),
    ("Bob", "Clothing", 350, 2),
]

# Create DataFrame
df = spark.createDataFrame(data, ["salesperson", "category", "amount", "quantity"])

print(f"Created DataFrame with {df.count()} rows")
df.show()

# =============================================================================
# 3. Basic DataFrame Operations
# =============================================================================

print("\n" + "=" * 80)
print("3. BASIC DATAFRAME OPERATIONS")
print("=" * 80)

# Show schema
print("DataFrame Schema:")
df.printSchema()

# Show first few rows
print("\nFirst 3 rows:")
df.show(3)

# =============================================================================
# 4. Filter and Select Operations
# =============================================================================

print("\n" + "=" * 80)
print("4. FILTER AND SELECT OPERATIONS")
print("=" * 80)

# Filter for high-value sales (amount > 500)
high_value_sales = df.filter(col("amount") > 500)

print("High-value sales (amount > 500):")
high_value_sales.show()

# Select specific columns
print("\nSalesperson and Amount only:")
df.select("salesperson", "amount").show(5)

# =============================================================================
# 5. Aggregations and Group By
# =============================================================================

print("\n" + "=" * 80)
print("5. AGGREGATIONS AND GROUP BY")
print("=" * 80)

# Total sales by salesperson
sales_by_person = df.groupBy("salesperson") \
    .agg(
        sum("amount").alias("total_sales"),
        count("*").alias("num_transactions"),
        avg("amount").alias("avg_sale_amount")
    ) \
    .orderBy(col("total_sales").desc())

print("Sales Summary by Salesperson:")
sales_by_person.show()

# Total sales by category
sales_by_category = df.groupBy("category") \
    .agg(
        sum("amount").alias("total_sales"),
        sum("quantity").alias("total_quantity")
    ) \
    .orderBy(col("total_sales").desc())

print("\nSales Summary by Category:")
sales_by_category.show()

# =============================================================================
# 6. Add Calculated Columns
# =============================================================================

print("\n" + "=" * 80)
print("6. ADD CALCULATED COLUMNS")
print("=" * 80)

# Add a calculated column for price per item
df_with_price = df.withColumn("price_per_item", col("amount") / col("quantity"))

# Add a category for sale size
df_enriched = df_with_price.withColumn(
    "sale_size",
    when(col("amount") > 1000, "Large")
    .when(col("amount") > 500, "Medium")
    .otherwise("Small")
)

print("Enriched DataFrame with calculated columns:")
df_enriched.show()

# =============================================================================
# 7. SQL Queries
# =============================================================================

print("\n" + "=" * 80)
print("7. SQL QUERIES")
print("=" * 80)

# Create a temporary view
df.createOrReplaceTempView("sales")

# Run SQL query
sql_result = spark.sql("""
    SELECT 
        salesperson,
        category,
        SUM(amount) as total_sales,
        COUNT(*) as num_transactions
    FROM sales
    WHERE amount > 400
    GROUP BY salesperson, category
    ORDER BY total_sales DESC
""")

print("SQL Query Results:")
sql_result.show()

# =============================================================================
# 8. Convert to Pandas (Optional)
# =============================================================================

print("\n" + "=" * 80)
print("8. CONVERT TO PANDAS (OPTIONAL)")
print("=" * 80)

# Convert to Pandas (requires pandas: pip install pandas)
try:
    pandas_df = sales_by_person.toPandas()
    print("Converted to Pandas DataFrame:")
    print(pandas_df)
except ImportError:
    print("Pandas not installed - skipping conversion")

# =============================================================================
# 9. Save Results (Optional)
# =============================================================================

print("\n" + "=" * 80)
print("9. SAVE RESULTS (OPTIONAL)")
print("=" * 80)

# Save to CSV (commented out - uncomment to run)
# sales_by_person.write.mode('overwrite').csv('output/sales_summary.csv', header=True)

# Save to Parquet (commented out - uncomment to run)
# df_enriched.write.mode('overwrite').parquet('output/sales_enriched.parquet')

print("✅ Script complete! You've successfully:")
print("  - Created a Spark session")
print("  - Created and manipulated DataFrames")
print("  - Performed aggregations and filtering")
print("  - Used SQL queries")
print("  - Worked with Spark DataFrames")

# =============================================================================
# 10. Cleanup
# =============================================================================

print("\n" + "=" * 80)
print("10. CLEANUP")
print("=" * 80)

# Stop the Spark session when done
spark.stop()
print("✅ Spark session stopped")

