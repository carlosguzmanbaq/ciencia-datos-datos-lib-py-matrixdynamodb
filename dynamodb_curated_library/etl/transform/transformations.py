import logging

from typing import Tuple, List, Optional
from pyspark.sql import DataFrame

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.extract.raw_sources import RawSources
from dynamodb_curated_library.etl.transform.curated_transformations import get_flatten_dynamodb_structuct_table
from dynamodb_curated_library.etl.transform.cdc.event_status import add_event_status_flag


logger = logging.getLogger(__name__)

class FlattenTransformations:
    """Esta clase tiene las transformaciones del job"""

    def __init__(self, sources: RawSources, job_config: JobConfig, max_depth: int = 3, columns_as_json: Optional[List[str]] = None):
        self.sources = sources
        self.job_config = job_config
        self.max_depth = max_depth
        self.columns_as_json = columns_as_json

    def get_flatten_table(self) -> Tuple[DataFrame, DataFrame, DataFrame, int]:
        """
        Get the main DynamoDB table DataFrame for processing.

        Returns:
            Tuple[DataFrame, DataFrame, DataFrame, int]: A tuple containing:
                - Flattened DynamoDB DataFrame
                - Keys DataFrame with primary keys + has_new_image flag
                - Raw DataFrame (original data for debugging)
                - Record count
        """
        df = self.sources.dynamodb_table

        result = get_flatten_dynamodb_structuct_table(
            df=df,
            job_config=self.job_config,
            max_depth=self.max_depth,
            columns_as_json=self.columns_as_json
        )

        if result is None:
            return None, None, None, 0

        df_flattend, df_keys, df_raw, record_count = result

        df_event_status = self.add_event_status(df_transform=df_flattend, df_keys=df_keys)

        if logger.isEnabledFor(logging.DEBUG):
            logger.info('\n\nDF Flatten Schema')
            df_event_status.printSchema()

        return df_event_status, df_keys, df_raw, record_count

    def add_event_status(self, df_transform: DataFrame, df_keys: DataFrame) -> DataFrame:
        """
        Add flag_event_status column if event_status is enabled in job_config.

        Args:
            df_transform: Transformed DataFrame (after business transformations)
            df_keys: Keys DataFrame with primary keys and has_new_image flag

        Returns:
            DataFrame with flag_event_status column added (if enabled)
        """
        if not self.job_config.event_status:
            logger.debug("event_status is disabled, skipping flag_event_status column")
            return df_transform

        logger.info("event_status is enabled, adding flag_event_status column")

        return add_event_status_flag(
            df_flatten=df_transform,
            df_keys=df_keys,
            catalog=self.sources.catalog,
            job_config=self.job_config
        )
