"""Defines Spark jobs in the BRONZE layer of the architecture"""
import json
import os

from pyspark.sql.types import StructType

minio_source_path = os.environ.get("BRONZE_DATA_PATH")
warehouse_path = os.environ.get("ICEBERG_WAREHOUSE_PATH")
TABLE_NAMESPACE = "exa.bronze"


def load_schema(schema_path: str) -> StructType:
    """
    Given a path to a schema, load the PySpark schema from a JSON file.

    Params:
        schema_path: path to the schema

    Returns:
        the schema object
    """
    with open(schema_path, encoding="utf-8") as f:
        schema_d = json.loads(f.read())
    return StructType.fromJson(schema_d)


if __name__ == "__main__":
    # generally considered bad practice, but common_spark provides Iceberg specific JARs
    # so importing it here means tests are more portable.
    from common_spark import spark

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {TABLE_NAMESPACE}")

    schema = load_schema(os.environ.get("SPARK_SCHEMA_PATH"))
    (
        spark.read.option("multiLine", True)
        .schema(schema)
        .json(minio_source_path)
        .write.format("iceberg")
        .mode("append")
        .option("checkpointLocation", f"{warehouse_path}/checkpoints/bronze/fhir")
        .saveAsTable(f"{TABLE_NAMESPACE}.fhir")
    )
