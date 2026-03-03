import os
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession

IS_LOCAL_ENV = os.environ.get('LOCAL_ENV', '').upper() == "DEV"


class LocalPathConverter:
    """Utility class for converting S3 paths to local development paths."""

    @staticmethod
    def s3_to_local(s3_path: str) -> str:
        """Convert S3 URI to local development path."""
        if not s3_path.startswith("s3://"):
            return s3_path
        return s3_path.replace("s3://", "dev/catalog/")

    @staticmethod
    def convert_paths(paths: List[str]) -> List[str]:
        """Convert list of S3 paths to local paths."""
        return [LocalPathConverter.s3_to_local(path) for path in paths]


class DevGlueReader:
    """Mock Glue reader for local development environment.

    This class provides local alternatives to AWS Glue's create_dynamic_frame
    and create_data_frame methods for development purposes.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialize DevGlueReader."""
        self.spark = spark

    def from_options(
        self,
        connection_options: Optional[Dict[str, Any]] = None,
        **_
    ):
        """Read data from file paths with S3 to local conversion."""
        if not connection_options or "paths" not in connection_options:
            raise ValueError("connection_options with 'paths' key is required")

        paths = connection_options["paths"]
        if not isinstance(paths, list):
            paths = [paths]

        local_paths = LocalPathConverter.convert_paths(paths)
        dataframe = self.spark.read.json(local_paths)

        def to_df():
            return dataframe

        dataframe.toDF = to_df
        return dataframe

    def from_catalog(
        self,
        database: str,
        table_name: str,
        **_
    ):
        """Read from Glue catalog using local Hive metastore."""
        if not database or not table_name:
            raise ValueError("Both database and table_name are required")

        dataframe = self.spark.table(f'{database}.{table_name}')

        def to_df():
            return dataframe

        dataframe.toDF = to_df
        return dataframe


class DevGlueContext:
    """Mock AWS Glue Context for local development.

    This class provides local alternatives to AWS Glue Context functionality,
    allowing developers to test ETL jobs locally without AWS Glue dependencies.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialize DevGlueContext."""
        reader = DevGlueReader(spark)
        self.create_data_frame = reader
        self.create_dynamic_frame = reader
        self.spark_session = spark
        setup_colombian_locale(spark._jvm)


def setup_colombian_locale(jvm) -> None:
    """Setup Colombian locale for Spark JVM.

    This function configures the JVM to use Colombian Spanish locale,
    which can be useful for date formatting and other locale-specific operations.
    """
    try:
        locale_class = jvm.java.util.Locale
        colombian_locale = locale_class("es", "CO")
        locale_class.setDefault(colombian_locale)

        default_locale = locale_class.getDefault()
        print(f"Default Locale set to: {default_locale}")
    except Exception as e:
        print(f"Warning: Could not set Colombian locale: {e}")
