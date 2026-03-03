import logging

from pyspark.sql import DataFrame

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.extract.raw_sources import RawSources
from dynamodb_curated_library.etl.transform.cdc.event_status import enrich_deleted_records
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.transform.business_transformations import get_transform_dynamodb_table

logger = logging.getLogger(__name__)


class DynamoDBTableTransformations:
    """
    Handles custom transformations for DynamoDB export data.

    This class encapsulates the transformation logic for processing flattened
    DynamoDB export data with business-specific rules.

    Attributes:
        df_flatten: Flattened DynamoDB DataFrame
        df_keys: DataFrame with primary keys + has_new_image flag
        df_raw: Original raw DataFrame for debugging/comparison
        job_config: Job configuration settings
        create_final_schema: Whether to print the final schema
    """

    def __init__(self, df_flatten: DataFrame, df_keys: DataFrame, df_raw: DataFrame, job_config: JobConfig, sources: RawSources, create_final_schema: bool = False):
        self.df_flatten = df_flatten
        self.df_keys = df_keys
        self.df_raw = df_raw
        self.job_config = job_config
        self.sources = sources
        self.create_final_schema = create_final_schema

    def get_main_table(self) -> DataFrame:
        """
        Apply transformations to get the final processed table.

        Returns:
            DataFrame: Transformed DataFrame with renamed columns, filtered nulls,
                      and timestamp casting applied
        """

        df_transform =  get_transform_dynamodb_table(
            df_flatten=self.df_flatten,
            df_keys=self.df_keys,
            job_config=self.job_config,
            create_final_schema=self.create_final_schema,
        )

        if not self.job_config.event_status:
            logger.debug("event_status is disabled, skipping flag_event_status column")
            return df_transform

        return enrich_deleted_records(
            df_with_status=df_transform,
            catalog=self.sources.catalog,
            job_config=self.job_config
        )
