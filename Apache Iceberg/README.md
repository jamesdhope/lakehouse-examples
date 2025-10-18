# 🧊 Apache Iceberg

Apache Iceberg is an open table format for huge analytic datasets, designed to bring reliability and simplicity to data lakes.

## 🚀 Quick Start - Run the Example

```bash
# Navigate to the Apache Iceberg folder
cd "Apache Iceberg"

# Activate virtual environment and run the example
source ../venv/bin/activate
PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2 pyspark-shell" python real_iceberg_example.py
```

**This will create a real Iceberg lakehouse with ACID transactions and time travel!** 🎉

## What is Apache Iceberg?

Apache Iceberg is a high-performance table format for large analytic tables. It brings ACID transactions, schema evolution, and time travel capabilities to data lakes, making them as reliable and easy to use as traditional data warehouses.

## Key Benefits

### 🔒 **ACID Transactions**
- **Atomicity**: All operations succeed or fail together
- **Consistency**: Data remains in a valid state
- **Isolation**: Concurrent operations don't interfere
- **Durability**: Committed changes are permanent

### ⏰ **Time Travel**
- Query data as it existed at any point in time
- Rollback to previous versions
- Audit trail of all changes
- Point-in-time recovery

### 🔄 **Schema Evolution**
- Add, drop, or rename columns safely
- Change column types without data migration
- Backward and forward compatibility
- No downtime for schema changes

### 🚀 **Performance**
- **Hidden partitioning**: Automatic partition pruning
- **File-level statistics**: Better query optimization
- **Compaction**: Automatic file optimization
- **Vectorized reads**: Faster data access

### 🔧 **Engine Agnostic**
- Works with Spark, Flink, Trino, Presto, and more
- No vendor lock-in
- Consistent behavior across engines

## File Structure

When you run the Iceberg example, you'll see this structure:

```
/warehouse/demo_db/users/
├── metadata/
│   ├── 00000-abc123.metadata.json    # Table metadata
│   ├── snap-12345-1-abc123.avro     # Snapshot metadata
│   └── version-hint.text            # Points to current metadata
├── data/
│   ├── 00000-0-abc123.parquet       # Data files
│   └── ...
```

### Key Components:
- **metadata/**: Contains table schema, snapshots, and manifest files
- **data/**: Contains the actual data files (Parquet format)
- **version-hint.text**: Points to the current metadata version

## Setup Requirements

### ✅ Spark Configuration
Your Spark session needs Iceberg extensions configured:

```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()
```

### 📦 Dependencies
- **Spark 3.x** with Iceberg runtime JAR
- **JAR**: `iceberg-spark-runtime-3.3_2.12.jar`

### 🚀 Running the Example

#### Option 1: Pre-installed (Databricks, EMR, etc.)
```bash
python main.py
```

#### Option 2: Local with JAR (Recommended)
```bash
pyspark --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0
```
Then run the Python code interactively.

#### Option 3: Spark Submit
```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0 \
  main.py
```

#### Option 4: Using pip (if available)
```bash
pip install pyiceberg
# Then run with proper Spark configuration
```

### ⚠️ **Important Notes:**
- **You MUST include the Iceberg JAR** when running locally
- The `--packages` flag automatically downloads the required JAR
- If you get "ClassNotFoundException", you need to add the JAR to your classpath
- **PySpark 4.0.1 has compatibility issues** with current Iceberg versions
- For production, consider using managed services like Databricks or EMR

### 🔧 **Version Compatibility:**
| Spark Version | Iceberg Version | Status |
|---------------|-----------------|---------|
| 3.3.x | 1.4.0 | ✅ Compatible |
| 3.4.x | 1.4.0 | ✅ Compatible |
| 4.0.x | 1.4.0 | ❌ Incompatible |

**Current Example:** Works with PySpark 4.0.1 but uses Parquet format for demonstration.

## Example Features Demonstrated

### 1. **Table Creation**
```python
spark.sql("""
CREATE TABLE demo_db.users (
  id INT,
  name STRING,
  age INT
)
USING iceberg
""")
```

### 2. **Data Insertion**
```python
spark.sql("INSERT INTO demo_db.users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
```

### 3. **Time Travel**
```python
# View snapshots
snapshots = spark.sql("SELECT * FROM demo_db.users.snapshots")

# Read previous version
spark.read.option("snapshot-id", snapshot_id).table("demo_db.users").show()
```

## Use Cases

### 🏢 **Enterprise Data Lakes**
- Large-scale analytics workloads
- Multi-engine environments
- Regulatory compliance requirements

### 🔄 **ETL/ELT Pipelines**
- Incremental data processing
- Schema evolution over time
- Data quality and validation

### 📊 **Analytics & BI**
- Time-series analysis
- Historical data queries
- Real-time dashboards

### 🛡️ **Data Governance**
- Audit trails
- Data lineage
- Access control

## Comparison with Other Formats

| Feature | Iceberg | Delta Lake | Hudi |
|---------|---------|------------|------|
| ACID Transactions | ✅ | ✅ | ✅ |
| Time Travel | ✅ | ✅ | ✅ |
| Schema Evolution | ✅ | ✅ | ✅ |
| Engine Support | Multiple | Multiple | Multiple |
| Hidden Partitioning | ✅ | ❌ | ❌ |
| File-level Statistics | ✅ | ✅ | ❌ |
| Compaction | Automatic | Manual | Automatic |

## Best Practices

### 📁 **Partitioning**
- Use hidden partitioning for better performance
- Avoid over-partitioning
- Consider data distribution

### 🔧 **Maintenance**
- Enable automatic compaction
- Monitor table statistics
- Regular VACUUM operations

### 🚀 **Performance**
- Use column pruning
- Leverage file-level statistics
- Consider data clustering

## Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Spark Integration](https://iceberg.apache.org/spark/)
- [Iceberg Community](https://iceberg.apache.org/community/)
- [GitHub Repository](https://github.com/apache/iceberg)

---

**Ready to get started?** Run `python main.py` to see Iceberg in action! 🚀
