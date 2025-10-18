from pyspark.sql import SparkSession

# Create Spark session with Hudi configuration
spark = SparkSession.builder \
    .appName("SimpleHudiExample") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .getOrCreate()

print("🚀 Spark session created successfully!")
print(f"Spark version: {spark.version}")

# Clean up any existing data
import shutil
import os
table_path = "/Users/jamesdhope/Documents/Projects/Lakehouse Examples/Hudi/hudi-warehouse"
if os.path.exists(table_path):
    shutil.rmtree(table_path)
    print("🧹 Cleaned up existing Hudi table")

# Create sample data using SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW temp_data AS SELECT * FROM VALUES (1, 'Alice', 30), (2, 'Bob', 25) AS t(id, name, age)")

# Write a Hudi table using DataFrame API instead of SQL
df = spark.sql("SELECT * FROM temp_data")

# Write as Hudi table
df.write \
    .format("hudi") \
    .option("hoodie.table.name", "hudi_table") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.partitionpath.field", "") \
    .option("hoodie.datasource.write.table.name", "hudi_table") \
    .option("hoodie.datasource.write.operation", "insert") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .mode("overwrite") \
    .save(table_path)

print("✅ Hudi table created successfully!")

# Read and show
result = spark.read.format("hudi").load(table_path)
print("📊 Initial data:")
result.select("id", "name", "age").show()

# Show the actual Hudi folder structure
print(f"\n📁 Actual Hudi folder structure created:")
print(f"   {table_path}/")
if os.path.exists(table_path):
    for root, dirs, files in os.walk(table_path):
        level = root.replace(table_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"   {indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            print(f"   {subindent}{file}")

print("\n🎉 Real Hudi example completed successfully!")
print("You now have a real Hudi table with ACID transactions!")
print(f"Check the folder: {table_path}")
