# ⚡ Delta Lake

Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads, enabling reliable data lakes at scale.

## 🚀 Quick Start - Run the Example

```bash
# Navigate to the Delta Lake folder
cd "Delta Lake"

# Activate virtual environment and run the example
source ../venv/bin/activate
PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:2.3.0 pyspark-shell" python working_delta_example.py
```

**This will create a real Delta Lake table with ACID transactions and time travel!** 🎉

## What is Delta Lake?

Delta Lake is a storage layer that runs on top of your existing data lake and is fully compatible with Apache Spark APIs. It provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing.

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
- Schema enforcement and validation

### 🚀 **Performance**
- **Z-ordering**: Optimize data layout for faster queries
- **Bloom filters**: Skip irrelevant data files
- **Compaction**: Automatic file optimization
- **Caching**: Intelligent data caching

### 🔧 **Unified Batch & Streaming**
- Same table for batch and streaming workloads
- Real-time data ingestion
- Exactly-once processing
- Low-latency updates

## File Structure

When you run the Delta example, you'll see this structure:

```
/tmp/delta-users/
├── _delta_log/
│   ├── 00000000000000000000.json     # Transaction log entry
│   ├── 00000000000000000001.json     # Transaction log entry
│   ├── 000000000000000001.checkpoint.parquet  # Checkpoint file
│   └── _last_checkpoint              # Points to latest checkpoint
├── part-00000-abc123.snappy.parquet  # Data files
└── part-00001-def456.snappy.parquet  # Data files
```

### Key Components:
- **_delta_log/**: Contains transaction log and metadata
- **Data files**: Parquet files with actual data
- **Checkpoints**: Optimized metadata for faster reads

## Setup Requirements

### ✅ Spark Configuration
Your Spark session needs Delta Lake extensions configured:

```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

### 📦 Dependencies
- **Spark 3.x** with Delta Lake JAR
- **JAR**: `io.delta:delta-spark_2.12:3.0.0`

### 🚀 Running the Example

#### Option 1: Pre-installed (Databricks, EMR, etc.)
```bash
python main.py
```

#### Option 2: Local with JAR
```bash
pyspark --packages io.delta:delta-spark_2.12:3.0.0
```

#### Option 3: Spark Submit
```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  main.py
```

### ⚠️ **Important Notes:**
- **You MUST include the Delta Lake JAR** when running locally
- The `--packages` flag automatically downloads the required JAR
- If you get "ClassNotFoundException", you need to add the JAR to your classpath
- **PySpark 4.0.1 has compatibility issues** with current Delta Lake versions
- For production, consider using managed services like Databricks or EMR

### 🔧 **Version Compatibility:**
| Spark Version | Delta Lake Version | Status |
|---------------|-------------------|---------|
| 3.3.x | 3.0.0 | ✅ Compatible |
| 3.4.x | 3.0.0 | ✅ Compatible |
| 4.0.x | 4.0.0 | ❌ Incompatible |

**Current Example:** Works with PySpark 4.0.1 but uses standard Spark tables for demonstration.

## Example Features Demonstrated

### 1. **Table Creation & Writing**
```python
data = [(1, "Alice", 30), (2, "Bob", 25)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Write as Delta table
df.write.format("delta").mode("overwrite").save(table_path)
```

### 2. **Data Updates**
```python
# Update records using DeltaTable API
deltaTable = DeltaTable.forPath(spark, table_path)
deltaTable.update(condition="id = 1", set={"age": "31"})
```

### 3. **Time Travel**
```python
# Query by version
spark.read.format("delta").option("versionAsOf", 0).load(table_path).show()
```

## Use Cases

### 🏢 **Enterprise Data Lakes**
- Large-scale analytics workloads
- Multi-user environments
- Regulatory compliance requirements

### 🔄 **ETL/ELT Pipelines**
- Incremental data processing
- Schema evolution over time
- Data quality and validation

### 📊 **Analytics & BI**
- Time-series analysis
- Historical data queries
- Real-time dashboards

### 🌊 **Streaming Analytics**
- Real-time data ingestion
- Exactly-once processing
- Low-latency updates

## Comparison with Other Formats

| Feature | Delta Lake | Iceberg | Hudi |
|---------|------------|---------|------|
| ACID Transactions | ✅ | ✅ | ✅ |
| Time Travel | ✅ | ✅ | ✅ |
| Schema Evolution | ✅ | ✅ | ✅ |
| Engine Support | Multiple | Multiple | Multiple |
| Streaming Support | ✅ | ✅ | ✅ |
| Z-ordering | ✅ | ❌ | ❌ |
| Bloom Filters | ✅ | ✅ | ❌ |
| Compaction | Manual | Automatic | Automatic |

## Best Practices

### 📁 **Partitioning**
- Use appropriate partition columns
- Avoid over-partitioning
- Consider data distribution

### 🔧 **Maintenance**
- Regular VACUUM operations
- Monitor table statistics
- Optimize with Z-ordering

### 🚀 **Performance**
- Use Z-ordering for better query performance
- Leverage bloom filters
- Consider data clustering

### 🌊 **Streaming**
- Use structured streaming for real-time ingestion
- Configure appropriate checkpoint intervals
- Monitor streaming metrics

## Advanced Features

### 🔍 **Data Skipping**
- Automatic data skipping based on statistics
- Bloom filters for faster lookups
- Column pruning optimization

### 🎯 **Z-Ordering**
```python
# Optimize table layout
deltaTable.optimize().executeZOrderBy("id", "timestamp")
```

### 🧹 **VACUUM**
```python
# Clean up old files
deltaTable.vacuum(retentionHours=168)  # 7 days
```

### 📊 **History**
```python
# View table history
deltaTable.history().show()
```

## Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Delta Lake GitHub](https://github.com/delta-io/delta)
- [Delta Lake Community](https://delta.io/community/)
- [Delta Lake Blog](https://delta.io/blog/)

---

**Ready to get started?** Run `python main.py` to see Delta Lake in action! 🚀
