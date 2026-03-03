from unittest.mock import Mock
import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session", name="spark")
def spark_feature():
    """Create Spark session for tests."""
    spark = (
        SparkSession.builder
        .appName("test_dynamodb_curated_library")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def glue_context(spark):
    """Create mock GlueContext for tests."""
    mock_glue = Mock()
    mock_glue.spark_session = spark
    return mock_glue
