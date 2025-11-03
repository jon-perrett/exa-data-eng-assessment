import json
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DataType, StructType

RESOURCE_TYPES = [
    "Patient",
    "Encounter",
    "CareTeam",
    "CarePlan",
    "DiagnosticReport",
    "DocumentReference",
    "Claim",
    "ExplanationOfBenefit",
    "Coverage",
    "AllergyIntolerance",
    "Condition",
]

minio_source_path = os.environ.get("BRONZE_DATA_PATH")
warehouse_path = os.environ.get("ICEBERG_WAREHOUSE_PATH")  # TODO: what if not exist?
table_namespace = "exa.silver"


def load_schema(schema_path: str) -> StructType:
    """
    Given a path to a schema, load the PySpark schema from a JSON file.

    Params:
        schema_path: path to the schema

    Returns:
        the schema object
    """
    with open(schema_path) as f:
        schema_d = json.loads(f.read())
    return StructType.fromJson(schema_d)


def get_complete_data_for_resource(
    resource_df: DataFrame, resource_type: str
) -> DataFrame:
    """
    Return the data for a particular resource type.

    Params:
        resource_df: dataframe containing only resources
        resource_type: the name of the resource type to extract

    Returns:
        dataframe containing only resources of the specified type
    """
    df = resource_df.filter(f"resourceType = '{resource_type}'")
    # Compute non-null counts for all columns
    # Doing .collect() isn't the most efficient, but it's ok for this small dataset
    non_null_counts = (
        df.select([F.count(c).alias(c) for c in df.columns]).collect()[0].asDict()
    )
    non_null_columns = [col for col, count in non_null_counts.items() if count > 0]
    return df.select(non_null_columns)


def explode_complex_fields(df_in: DataFrame) -> DataFrame:
    """
    Our dataset contains a number of "complex" fields, i.e. ArrayType[StructType] and StructType.

    Aim to end up with exploded struct fields which contain either a single value, or an array of values.
    We don't do this recursively, as this becomes a very expensive operation and provides less value
    for the "silver" dataset.

    Params:
        df_in: input dataframe

    Returns:
        dataframe containing exploded fields
    """
    new_cols = (
        df_in.columns.copy()
    )  # copy so that changes to the dataframe do not cause issues

    for field in df_in.schema.fields:
        col_name = field.name
        col_type = field.dataType

        if (
            isinstance(col_type, ArrayType)
            and isinstance(col_type.elementType, StructType)
        ) or isinstance(col_type, StructType):
            new_cols.remove(col_name)
            new_cols.extend(get_struct_columns(col_name, col_type))

    return df_in.select(*new_cols)


def get_struct_columns(col_name: str, col_type: DataType) -> list[Column]:
    """
    Given an ArrayType or StructType, return the columns for each StructType field.

    Params:
        col_name: name of the original column
        col_type: the type of the original column

    Returns:
        column expressions to transform each column
    """
    if isinstance(col_type, ArrayType):
        struct_fields = col_type.elementType.names
        return [
            F.expr(f"transform({col_name}, x -> x.{f})").alias(f"{col_name}_{f}")
            for f in struct_fields
        ]
    else:
        struct_fields = col_type.names
        return [
            F.col(f"{col_name}.{f}").alias(f"{col_name}_{f}") for f in struct_fields
        ]


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("FHIR Silver Iceberg Writer")
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
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table_namespace}")

    schema = load_schema(os.environ.get("SPARK_SCHEMA_PATH"))
    df = spark.read.option("multiLine", True).schema(schema).json(minio_source_path)

    resource_df = df.select(F.explode("entry").alias("entry")).select(
        "entry.resource.*"
    )

    (
        explode_complex_fields(resource_df)
        .write.format("iceberg")
        .mode("append")
        .option("checkpointLocation", f"{warehouse_path}/checkpoints/resources")
        .saveAsTable(f"{table_namespace}.resources")
    )
