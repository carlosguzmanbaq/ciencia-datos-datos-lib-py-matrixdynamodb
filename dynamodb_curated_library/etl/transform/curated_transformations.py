from typing import Tuple, Optional, List
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import flatten_dynamodb_struct, to_snake_case
from dynamodb_curated_library.etl.transform.utils.dynamodb_value_extractor import extract_dynamodb_value

logger = logging.getLogger(__name__)


def get_flatten_dynamodb_structuct_table(df: DataFrame, job_config: JobConfig, max_depth: int, columns_as_json: Optional[List[str]]) -> Optional[Tuple[DataFrame, DataFrame, DataFrame, int]]:
    """
    Process and flatten DynamoDB export data.

    Handles both FULL and INCREMENTAL exports by extracting primary keys and flattening
    the DynamoDB structure into a normalized DataFrame.

    Args:
        df: Raw DynamoDB export DataFrame
        job_config: Job configuration with export type information
        max_depth: Maximum depth level to flatten (default: 3). Deeper structures will be converted to JSON string
        columns_as_json: List of column paths to keep as JSON string instead of flattening
                        Example: ["state.M.orderSteps", "metadata.M.details"]

    Returns:
        Tuple containing:
            - Flattened DataFrame with normalized DynamoDB data
            - Keys DataFrame with primary keys + has_new_image flag (for event_status)
            - Raw DataFrame (original data for debugging/comparison)
            - Record count (number of records processed)

        Returns None if DataFrame is empty
    """
    record_count = df.count()
    if record_count == 0:
        return None

    logger.info("DF Origin Record Count = %d", record_count)

    # Keep raw DataFrame for debugging/comparison
    df_raw = df

    column_to_validate = "Item"
    primary_keys = job_config.constants.primary_keys

    if job_config.dynamodb_export.is_incremental:
        # Incremental: Extract keys from Keys field + NewImage indicator
        select_expressions = [
            extract_dynamodb_value(df, f"Keys.{pk}", to_snake_case(pk))
            for pk in primary_keys
        ]
        df_keys = df.select(*select_expressions, F.col("NewImage").isNotNull().alias("has_new_image"))
        column_to_validate = "NewImage"

        # Filter DF events when it is equally deleted, because <flatten_dynamodb_struct> cannot flatten only PK
        if job_config.event_status:
            df = df.filter(F.col("NewImage").isNotNull())
    else:
        # Full: Extract keys from Item
        select_expressions = [
            extract_dynamodb_value(df, f"Item.{pk}", to_snake_case(pk))
            for pk in primary_keys
        ]
        df_keys = df.select(*select_expressions)

    logger.debug("Origin DF Schema:")
    if logger.isEnabledFor(logging.DEBUG):
        df.printSchema()

    df_flatten_dynamod = flatten_dynamodb_struct(
        df=df,
        max_depth=max_depth,
        parent_col=column_to_validate,
        columns_as_json=columns_as_json
    )

    return df_flatten_dynamod, df_keys, df_raw, record_count
