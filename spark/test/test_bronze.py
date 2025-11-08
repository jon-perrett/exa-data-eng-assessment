import json
import tempfile
from pathlib import Path

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from jobs.bronze import load_schema


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
