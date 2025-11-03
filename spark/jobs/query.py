import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

warehouse_path = os.environ.get("ICEBERG_WAREHOUSE_PATH")  # TODO: what if not exist?
table_name = "exa.silver.fhir_resources"
spark = (
    SparkSession.builder
    .appName("FHIR Reader")
    .config("spark.sql.catalog.exa", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.exa.uri", "http://nessie:19120/api/v2")
    .config("spark.sql.catalog.exa.ref", "main")
    .config("spark.sql.catalog.exa.warehouse", warehouse_path)
    .config("spark.sql.catalog.exa.type", "nessie")
    .config("spark.sql.catalog.exa.authentication.type", "NONE")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sql("""
          SELECT concat_ws(' ', expName.prefix[0], expName.given[0], expName.family) fullName,
            expAddr.city,
            expAddr.country,
            array_join(expAddr.line, ' ') addressLines,
            expAddr.postalCode,
            expAddr.state
          FROM (
            SELECT explode(name) expName, explode(address) expAddr
            FROM exa.silver.fhir_resources
            WHERE resourceType = 'Patient'
          ) 
          WHERE expName.use = 'official'
          LIMIT 10""").show()
