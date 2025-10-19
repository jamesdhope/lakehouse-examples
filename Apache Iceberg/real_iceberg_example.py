from pyspark.sql import SparkSession

# Create Spark session with Iceberg extensions
# This will work with the Iceberg JAR loaded via --packages
spark = SparkSession.builder \
    .appName("RealIcebergExample") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg-warehouse") \
    .getOrCreate()

print("🚀 Spark session created successfully!")
print(f"Spark version: {spark.version}")

# Clean up any existing data
import shutil
import os
warehouse_path = "/tmp/iceberg-warehouse"
if os.path.exists(warehouse_path):
    shutil.rmtree(warehouse_path)
    print("🧹 Cleaned up existing Iceberg warehouse")

try:
    # Create database first
    spark.sql("CREATE DATABASE IF NOT EXISTS demo_db")
    print("✅ Database created successfully!")
    
    # Create an Iceberg table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS demo_db.users (
      id INT,
      name STRING,
      age INT
    )
    USING iceberg
    """)
    
    print("✅ Iceberg table created successfully!")
    
    # Insert data
    spark.sql("INSERT INTO demo_db.users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
    print("✅ Data inserted successfully!")
    
    # Query data
    result = spark.sql("SELECT * FROM demo_db.users")
    print("📊 Query results:")
    result.show()
    
    # Add more data
    spark.sql("INSERT INTO demo_db.users VALUES (3, 'Charlie', 35)")
    print("✅ Additional data inserted!")
    
    # Final query
    final_result = spark.sql("SELECT * FROM demo_db.users")
    print("📊 Final results:")
    final_result.show()
    
    # Show snapshots (Iceberg feature)
    print("📸 Table snapshots:")
    snapshots = spark.sql("SELECT * FROM demo_db.users.snapshots")
    snapshots.show()
    
    # Time Travel Queries - Query different snapshots
    print("\n🕰️ TIME TRAVEL DEMONSTRATION:")
    
    # Get snapshot IDs for time travel
    snapshot_data = snapshots.collect()
    if len(snapshot_data) >= 2:
        # Query the first snapshot (original data: Alice, Bob)
        first_snapshot_id = snapshot_data[0]['snapshot_id']
        print(f"📊 Querying snapshot {first_snapshot_id} (original data):")
        spark.sql(f"SELECT * FROM demo_db.users VERSION AS OF {first_snapshot_id}").show()
        
        # Query the second snapshot (with Charlie added)
        second_snapshot_id = snapshot_data[1]['snapshot_id']
        print(f"📊 Querying snapshot {second_snapshot_id} (with Charlie):")
        spark.sql(f"SELECT * FROM demo_db.users VERSION AS OF {second_snapshot_id}").show()
    
    # Time travel by timestamp (if you know the approximate time)
    print("\n⏰ Time travel by timestamp (last 5 minutes):")
    try:
        spark.sql("SELECT * FROM demo_db.users VERSION AS OF '2025-10-18 19:10:00'").show()
    except:
        print("   (Timestamp-based time travel requires exact timestamp)")
    
    # Compare snapshots side by side
    print("\n🔄 Comparing snapshots:")
    if len(snapshot_data) >= 2:
        print("Snapshot 1 (original):")
        spark.sql(f"SELECT * FROM demo_db.users VERSION AS OF {snapshot_data[0]['snapshot_id']}").show()
        print("Snapshot 2 (with additions):")
        spark.sql(f"SELECT * FROM demo_db.users VERSION AS OF {snapshot_data[1]['snapshot_id']}").show()
    
    # Show history of changes
    print("\n📜 Table history:")
    history = spark.sql("SELECT * FROM demo_db.users.history")
    history.show()
    
    # Rollback example (creates a new snapshot)
    print("\n🔄 Rollback to first snapshot (creates new snapshot):")
    if len(snapshot_data) >= 1:
        spark.sql(f"CALL spark_catalog.system.rollback_to_snapshot('demo_db.users', {snapshot_data[0]['snapshot_id']})")
        print("📊 Data after rollback:")
        spark.sql("SELECT * FROM demo_db.users").show()
    
    # Show the actual Iceberg folder structure that was created
    print(f"\n📁 Actual Iceberg folder structure created:")
    print(f"   {warehouse_path}/")
    if os.path.exists(warehouse_path):
        for root, dirs, files in os.walk(warehouse_path):
            level = root.replace(warehouse_path, '').count(os.sep)
            indent = ' ' * 2 * level
            print(f"   {indent}{os.path.basename(root)}/")
            subindent = ' ' * 2 * (level + 1)
            for file in files:
                print(f"   {subindent}{file}")
    
    print("\n🎉 Real Iceberg example completed successfully!")
    print("You now have a real Iceberg table with ACID transactions and time travel!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("\n💡 Make sure to run this with the Iceberg JAR loaded:")
    print("PYSPARK_SUBMIT_ARGS='--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0 pyspark-shell' python real_iceberg_example.py")
