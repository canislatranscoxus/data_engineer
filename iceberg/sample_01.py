from pyspark.sql import SparkSession

# Initialize Spark with Apache Iceberg packages and catalogs
spark = SparkSession.builder \
    .appName("IcebergLocalLearning") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "./iceberg_warehouse") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Write to a local Iceberg table
df.write.format("iceberg").saveAsTable("local.db.users")

# Read it back to verify
spark.read.format("iceberg").load("local.db.users").show()
