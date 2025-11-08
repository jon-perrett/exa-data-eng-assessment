"""Defines Spark jobs in the SILVER layer of the architecture"""
import os

from pyspark.sql import Column, DataFrame
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
warehouse_path = os.environ.get("ICEBERG_WAREHOUSE_PATH")

SILVER_NAMESPACE = "exa.silver"


def get_complete_data_for_resource(
    df: DataFrame, resource_type: str
) -> DataFrame:
    """
    Return the data for a particular resource type.

    Params:
        df: dataframe containing only resources
        resource_type: the name of the resource type to extract

    Returns:
        dataframe containing only resources of the specified type
    """
    df = df.filter(f"resourceType = '{resource_type}'")
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

    Aim to end up with exploded struct fields which contain either a single value, or an array
    of values. We don't do this recursively, as this becomes a very expensive operation.

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
    struct_fields = col_type.names
    return [
        F.col(f"{col_name}.{f}").alias(f"{col_name}_{f}") for f in struct_fields
    ]


if __name__ == "__main__":
    # generally considered bad practice, but common_spark provides Iceberg specific JARs,
    # so importing it here means tests are more portable.
    from common_spark import spark

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}").show()
    df = spark.read.format("iceberg").load("exa.bronze.fhir")

    resource_df = df.select(F.explode("entry").alias("entry")).select(
        "entry.resource.*"
    )

    for resource_type in RESOURCE_TYPES:
        (
            explode_complex_fields(
                resource_df.filter(f"resourceType = '{resource_type}'")
            )
            .repartition(10, "patient_reference", "id")  # partition by common join keys
            .write.format("iceberg")
            .mode("append")
            .option("checkpointLocation", f"{warehouse_path}/checkpoints/bronze/fhir")
            .saveAsTable(f"{SILVER_NAMESPACE}.{resource_type.lower()}")
        )
