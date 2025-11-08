import json
import tempfile
from pathlib import Path

import pytest
from spark.jobs.bronze import get_complete_data_for_resource, get_struct_columns, load_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def test_load_schema():
    original = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    with tempfile.NamedTemporaryFile("w", delete=False) as tf:
        json.dump(original.jsonValue(), tf)
        tf_path = tf.name

    try:
        loaded = load_schema(tf_path)
        assert isinstance(loaded, StructType)
        assert loaded.jsonValue() == original.jsonValue()
    finally:
        Path(tf_path).unlink(missing_ok=True)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


def test_get_complete_data_for_resource(spark):
    rows = [
        {
            "resourceType": "Patient",
            "id": "p1",
            "name": "Alice",
            "age": 30,
            "notes": None,
        },
        {
            "resourceType": "Patient",
            "id": "p2",
            "name": None,
            "age": None,
            "notes": None,
        },
        {
            "resourceType": "Encounter",
            "id": "e1",
            "name": "E",
            "age": None,
            "notes": "x",
        },
    ]
    df = spark.createDataFrame(rows)
    result = get_complete_data_for_resource(df, "Patient")

    # Expect columns that have at least one non-null value for Patient rows
    assert "notes" not in result.columns
    expected_cols = {"resourceType", "id", "name", "age"}
    assert set(result.columns) == expected_cols
    assert result[result.resourceType == "Patient"].count() == result.count()


def test_get_struct_columns_with_struct(spark):
    schema = StructType(
        [
            StructField(
                "person",
                StructType(
                    [
                        StructField("first", StringType(), True),
                        StructField("age", IntegerType(), True),
                    ]
                ),
                True,
            )
        ]
    )
    rows = [
        {"person": {"first": "Alice", "age": 30}},
        {"person": {"first": "Bob", "age": 25}},
    ]
    df = spark.createDataFrame(rows, schema=schema)

    col_type = schema.fields[0].dataType
    cols = get_struct_columns("person", col_type)

    out = df.select(*cols).collect()
    assert [row["person_first"] for row in out] == ["Alice", "Bob"]
    assert [row["person_age"] for row in out] == [30, 25]


def test_get_struct_columns_with_array_of_struct(spark):
    addr_struct = StructType(
        [
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True),
        ]
    )
    schema = StructType([StructField("addresses", ArrayType(addr_struct), True)])
    rows = [
        {"addresses": [{"city": "NY", "zip": "10001"}, {"city": "LA", "zip": "90001"}]},
        {"addresses": [{"city": "SF", "zip": "94101"}]},
    ]
    df = spark.createDataFrame(rows, schema=schema)

    col_type = schema.fields[0].dataType
    cols = get_struct_columns("addresses", col_type)

    out = df.select(*cols).collect()
    assert out[0]["addresses_city"] == ["NY", "LA"]
    assert out[0]["addresses_zip"] == ["10001", "90001"]
    assert out[1]["addresses_city"] == ["SF"]
    assert out[1]["addresses_zip"] == ["94101"]
