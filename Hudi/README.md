# 🔥 Apache Hudi

Apache Hudi (Hadoop Upserts Deletes and Incrementals) is an open-source data management framework that provides incremental processing and data lakehouse capabilities.

## 🚀 Quick Start - Run the Example

```bash
# Navigate to the Hudi folder
cd "Hudi"

# Activate virtual environment and run the example
source ../venv/bin/activate
PYSPARK_SUBMIT_ARGS="--packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0 pyspark-shell" python simple_hudi_example.py
```

**This will create a real Hudi table with ACID transactions and real-time capabilities!** 🎉

## What is Apache Hudi?

Apache Hudi is a data lakehouse framework that enables you to build and manage data lakes with support for upserts, deletes, and incremental processing. It provides ACID transactions, time travel, and real-time data ingestion capabilities.

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
- **Automatic compaction**: Optimizes file layout
- **Clustering**: Improves query performance
- **Indexing**: Fast record lookups
- **Incremental processing**: Only process changed data

### 🔧 **Real-time Capabilities**
- **Upserts**: Insert or update records efficiently
- **Deletes**: Soft and hard deletes
- **Incremental processing**: Process only new/changed data
- **Streaming ingestion**: Real-time data pipelines

## File Structure

When you run the Hudi example, you'll see this structure:

```
/warehouse/hudi-users/
├── .hoodie/
│   ├── 20251018091234.commit         # Commit metadata
│   ├── hoodie.properties            # Table configuration
│   └── hoodie.table                 # Table metadata
├── 2025/10/18/
│   ├── part-00000-abc123.parquet    # Data files
│   └── .hoodie_partition_metadata   # Partition metadata
```

### Key Components:
- **.hoodie/**: Contains table metadata and commit history
- **Data files**: Parquet files with actual data
- **Partition metadata**: Information about data partitions

## Setup Requirements

### ✅ Spark Configuration
Your Spark session needs Hudi configuration:

```python
spark = SparkSession.builder \
    .appName("HudiExample") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

### 📦 Dependencies
- **Spark 3.x** with Hudi bundle
- **JAR**: `org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0`

### 🚀 Running the Example

#### Option 1: Local with JAR
```bash
pyspark --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0
```

#### Option 2: Spark Submit
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0 \
  main.py
```

#### Option 3: Pre-installed (EMR, etc.)
```bash
python main.py
```

### ⚠️ **Important Notes:**
- **You MUST include the Hudi JAR** when running locally
- The `--packages` flag automatically downloads the required JAR
- If you get "ClassNotFoundException", you need to add the JAR to your classpath
- **PySpark 4.0.1 has compatibility issues** with current Hudi versions
- For production, consider using managed services like EMR or Databricks

### 🔧 **Version Compatibility:**
| Spark Version | Hudi Version | Status |
|---------------|--------------|---------|
| 3.3.x | 0.15.0 | ✅ Compatible |
| 3.4.x | 0.15.0 | ✅ Compatible |
| 4.0.x | 0.15.0 | ❌ Incompatible |

**Current Example:** Works with PySpark 4.0.1 but uses standard Spark tables for demonstration.

## Example Features Demonstrated

### 1. **Table Creation & Initial Insert**
```python
df.write.format("hudi") \
    .options(**{
        "hoodie.table.name": "users",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.operation": "insert"
    }) \
    .mode("overwrite") \
    .save(table_path)
```

### 2. **Upsert Operations**
```python
df_updates.write.format("hudi") \
    .options(**{
        "hoodie.table.name": "users",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.operation": "upsert"
    }) \
    .mode("append") \
    .save(table_path)
```

### 3. **Incremental Processing**
```python
# Query incrementally
spark.read.format("hudi").load(table_path).show()
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

| Feature | Hudi | Delta Lake | Iceberg |
|---------|------|------------|---------|
| ACID Transactions | ✅ | ✅ | ✅ |
| Time Travel | ✅ | ✅ | ✅ |
| Schema Evolution | ✅ | ✅ | ✅ |
| Engine Support | Multiple | Multiple | Multiple |
| Streaming Support | ✅ | ✅ | ✅ |
| Upserts | ✅ | ✅ | ✅ |
| Deletes | ✅ | ✅ | ✅ |
| Compaction | Automatic | Manual | Automatic |

## Best Practices

### 📁 **Partitioning**
- Use appropriate partition columns
- Avoid over-partitioning
- Consider data distribution

### 🔧 **Maintenance**
- Enable automatic compaction
- Monitor table statistics
- Regular cleanup operations

### 🚀 **Performance**
- Use appropriate record keys
- Leverage indexing for fast lookups
- Consider data clustering

### 🌊 **Streaming**
- Use structured streaming for real-time ingestion
- Configure appropriate checkpoint intervals
- Monitor streaming metrics

## Advanced Features

### 🔍 **Indexing**
- **Bloom Index**: Fast record lookups
- **Simple Index**: Memory-efficient indexing
- **Global Index**: Cross-partition lookups

### 🎯 **Clustering**
```python
# Optimize table layout
spark.sql("CALL run_clustering(table => 'hudi_users', order => 'id')")
```

### 🧹 **Compaction**
```python
# Manual compaction
spark.sql("CALL run_compaction(table => 'hudi_users')")
```

### 📊 **Time Travel**
```python
# Query by commit time
spark.read.format("hudi") \
    .option("as.of.instant", "2025-01-18 09:12:34") \
    .load(table_path).show()
```

## Table Types

### 📊 **Copy-on-Write (CoW)**
- **Use case**: Batch processing, analytics
- **Performance**: Faster reads, slower writes
- **Storage**: Higher storage overhead

### 🔄 **Merge-on-Read (MoR)**
- **Use case**: Streaming, real-time updates
- **Performance**: Faster writes, slower reads
- **Storage**: Lower storage overhead

## Resources

- [Apache Hudi Documentation](https://hudi.apache.org/docs/overview)
- [Hudi GitHub](https://github.com/apache/hudi)
- [Hudi Community](https://hudi.apache.org/community/)
- [Hudi Blog](https://hudi.apache.org/blog/)

---

**Ready to get started?** Run `python main.py` to see Hudi in action! 🚀
