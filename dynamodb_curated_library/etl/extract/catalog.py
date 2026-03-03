import logging
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from awsglue.context import GlueContext

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.extract.config_tables.config_table import ConfigTable
from dynamodb_curated_library.dev_utils.schema_formatter import print_formatted_schema

logger = logging.getLogger(__name__)


class CatalogException(Exception):
    """Class to handle exceptions from the Catalog class"""


class Catalog:
    """Class that gets the tables"""
    def __init__(self, glue_context: GlueContext, job_config: JobConfig, schema: Optional[StructType] = None, create_full_schema: bool = False):
        """
        Initialize Catalog for reading DynamoDB export data from S3.

        Args:
            glue_context: AWS Glue context for data operations
            job_config: Job configuration with process type and table settings
            schema: Optional predefined schema for FULL loads to avoid OOM (Out of Memory)
            create_full_schema: If True, prints the DataFrame schema for schema generation
        """
        self.glue_context = glue_context
        self.job_config = job_config
        self.config_table = ConfigTable(self.job_config)
        self.schema = schema
        self.create_full_schema = create_full_schema

    def get_table(self) -> DataFrame:
        """
        Get the DynamoDB table data from S3.
        Uses defined schema for FULL loads (avoids OOM) and inferred schema for INCREMENTAL loads.

        Returns:
            DataFrame with the table data

        Raises:
            CatalogException: If there's an error reading the table from S3
        """
        source_meta = self.config_table.get_source_options()
        is_full = self.job_config.process_type == "FULL"

        try:
            logger.info("Reading S3 Table: %s", source_meta.s3_uri)

            if is_full and self.schema:
                table_df = self.glue_context.create_data_frame.from_options(
                    connection_type="s3",
                    format="json",
                    infer_schema=False,
                    connection_options={
                        "paths": [source_meta.s3_uri],
                        "recurse": True,
                        "exclusions": '["**manifest**"]',
                    },
                    schema=self.schema
                )
            else:
                table_df = self.glue_context.create_data_frame.from_options(
                    connection_type="s3",
                    format="json",
                    infer_schema=True,
                    connection_options={
                        "paths": [source_meta.s3_uri],
                        "recurse": True,
                        "exclusions": '["**manifest**"]',
                    },
                )

            if self.create_full_schema:
                print_formatted_schema(table_df.schema)

            return table_df

        except Exception as e:
            error_msg = f'Error reading files from {source_meta.s3_uri}'
            logger.error(error_msg)
            raise CatalogException(error_msg) from e

    def get_dataframe_from_catalog(self, job_config: JobConfig, select_columns: Optional[list] = None) -> DataFrame:
        """
        Read DataFrame from Glue Catalog.

        Args:
            job_config: Job configuration with database and table information
            select_columns: Optional list of columns to select (for performance)

        Returns:
            DataFrame from catalog

        Raises:
            CatalogException: If there's an error reading from Glue Catalog
        """
        try:
            logger.info(
                "Reading from Glue Catalog - Database: %s, Table: %s",
                job_config.table_catalog_names.curated_database,
                job_config.table_name
            )

            df: DataFrame = self.glue_context.create_data_frame.from_catalog(
                database=job_config.table_catalog_names.curated_database,
                table_name=job_config.table_name
            )

            if select_columns:
                df = df.select(*select_columns)
                logger.debug("Selected columns: %s", select_columns)

            return df

        except Exception as e:
            error_msg = (
                f"Error reading from Glue Catalog - "
                f"Database: {job_config.table_catalog_names.curated_database}, "
                f"Table: {job_config.table_name}"
            )
            logger.error(error_msg)
            raise CatalogException(error_msg) from e
