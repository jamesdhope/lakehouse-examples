from pyspark.sql import SparkSession

# Create Spark session with Delta Lake extensions
spark = SparkSession.builder \
    .appName("WorkingDeltaExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("🚀 Spark session created successfully!")
print(f"Spark version: {spark.version}")

# Clean up any existing data
import shutil
import os
table_path = "/Users/jamesdhope/Documents/Projects/Lakehouse Examples/Delta Lake/delta-warehouse"
if os.path.exists(table_path):
    shutil.rmtree(table_path)
    print("🧹 Cleaned up existing Delta table")

# Create sample data using SQL instead of DataFrame to avoid serialization issues
spark.sql("CREATE OR REPLACE TEMPORARY VIEW temp_data AS SELECT * FROM VALUES (1, 'Alice', 30), (2, 'Bob', 25) AS t(id, name, age)")

# Write a Delta table
spark.sql(f"""
CREATE TABLE delta_table
USING DELTA
LOCATION '{table_path}'
AS SELECT * FROM temp_data
""")

print("✅ Delta table created successfully!")

# Read and show
result = spark.sql("SELECT * FROM delta_table")
print("📊 Initial data:")
result.show()

# Update a record
spark.sql("UPDATE delta_table SET age = 31 WHERE id = 1")
print("✅ Record updated successfully!")

# Show data after update
print("📊 Data after update:")
spark.sql("SELECT * FROM delta_table").show()

# Time travel (query by version)
print("📊 Time travel - version 0 (original data):")
spark.sql("SELECT * FROM delta_table VERSION AS OF 0").show()

# Show the actual Delta Lake folder structure
print(f"\n📁 Actual Delta Lake folder structure created:")
print(f"   {table_path}/")
if os.path.exists(table_path):
    for root, dirs, files in os.walk(table_path):
        level = root.replace(table_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"   {indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            print(f"   {subindent}{file}")

print("\n🎉 Real Delta Lake example completed successfully!")
print("You now have a real Delta Lake table with ACID transactions and time travel!")
print(f"Check the folder: {table_path}")
