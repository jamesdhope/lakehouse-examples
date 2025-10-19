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

# Add more data to create multiple commits
print("\n➕ Adding more data...")
new_data = spark.sql("SELECT * FROM VALUES (3, 'Charlie', 35), (4, 'Diana', 28) AS t(id, name, age)")
new_data.write \
    .format("hudi") \
    .option("hoodie.table.name", "hudi_table") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.partitionpath.field", "") \
    .option("hoodie.datasource.write.table.name", "hudi_table") \
    .option("hoodie.datasource.write.operation", "insert") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .mode("append") \
    .save(table_path)

print("✅ Additional data inserted!")

# Show current data
print("📊 Current data:")
current_result = spark.read.format("hudi").load(table_path)
current_result.select("id", "name", "age").show()

# Time Travel Queries - Query different commits
print("\n🕰️ TIME TRAVEL DEMONSTRATION:")

# Show commit timeline using DataFrame operations
print("📜 Commit timeline:")
try:
    # Get all unique commit times from the data
    commits = current_result.select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time").collect()
    print("Available commits:")
    for i, commit in enumerate(commits):
        print(f"  Commit {i}: {commit['_hoodie_commit_time']}")
except Exception as e:
    print(f"   (Commit timeline: {e})")

# Time travel by commit time
print("\n⏰ Time travel by commit time:")
try:
    # Get commit times from the data
    commits = current_result.select("_hoodie_commit_time").distinct().collect()
    if len(commits) >= 2:
        # Query first commit (oldest)
        first_commit = commits[0]['_hoodie_commit_time']
        print(f"📊 Data at commit {first_commit}:")
        first_commit_data = spark.read.format("hudi").option("as.of.instant", first_commit).load(table_path)
        first_commit_data.select("id", "name", "age").show()
        
        # Query second commit (newest)
        second_commit = commits[1]['_hoodie_commit_time']
        print(f"📊 Data at commit {second_commit}:")
        second_commit_data = spark.read.format("hudi").option("as.of.instant", second_commit).load(table_path)
        second_commit_data.select("id", "name", "age").show()
        
        # Compare commits
        print("\n🔄 Comparing commits:")
        print("First commit data:")
        first_commit_data.select("id", "name", "age").show()
        print("Second commit data:")
        second_commit_data.select("id", "name", "age").show()
        
except Exception as e:
    print(f"   (Time travel by commit: {e})")

# Show Hudi-specific metadata
print("\n🔍 Hudi-specific features:")
print("📊 Hudi metadata columns:")
try:
    # Show all columns including Hudi metadata
    print("All columns in the table:")
    current_result.printSchema()
    print("\nSample data with Hudi metadata:")
    current_result.show(truncate=False)
except Exception as e:
    print(f"   (Metadata display: {e})")

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
