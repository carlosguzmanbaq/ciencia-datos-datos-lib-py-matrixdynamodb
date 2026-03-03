import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.dev_utils.schema_formatter import print_formatted_schema
from dynamodb_curated_library.etl.transform.utils.align_schema import align_schema

from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.schemas.co_dynamodb_surprise_gift_redeems import final_schema
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.transform.utils.cast_timestamp import cast_column_to_timestamp
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.transform.utils.exceptions import TransformationException

logger = logging.getLogger(__name__)


def get_transform_dynamodb_table(df_flatten: DataFrame, df_keys: DataFrame, job_config: JobConfig, create_final_schema: bool) -> DataFrame:
    """
    Transform flattened DynamoDB data with column renaming, filtering, and type casting.

    Args:
        df_flatten: Flattened DataFrame from DynamoDB export
        df_keys: DataFrame with primary keys + has_new_image flag
        job_config: Job configuration
        create_final_schema: Whether to print the final schema

    Returns:
        Transformed DataFrame with renamed columns, filtered nulls, and timestamp casting

    Raises:
        TransformationException: If transformation fails
    """
    try:

        # Left anti join to validate if there are any PKs not present in the keys DataFrame
        df_missing_pks = df_keys.join(
            df_flatten,
            on=["client_id", "tstamp"],
            how="leftanti"
        )
        missing_pk_count = df_missing_pks.count()

        logger.info("Total missing PKs: %d", missing_pk_count)
        if missing_pk_count > 0:
            logger.warning("Found %d records with missing PKs", missing_pk_count)
            df_missing_pks.show(vertical=True, truncate=False, n=missing_pk_count)

        # Add id_reward column as the primary key for the table
        df_with_unique_key = df_flatten.withColumn("id_reward", F.concat_ws("_", F.col("client_id"), F.col("tstamp")))

        df_cast_decimal = df_with_unique_key.withColumn(
            "value",
            F.col("value").cast(T.DecimalType(36, 6))
        )

        # Cast tstamp a timestamp
        df_with_timetamp = cast_column_to_timestamp(df=df_cast_decimal, column_name="tstamp")

        if create_final_schema:
            print_formatted_schema(df_with_timetamp.schema)

        df_result = align_schema(df_with_timetamp, final_schema)
        return df_result

    except Exception as e:
        logger.error("Unexpected error during transformation: %s", str(e))
        raise TransformationException(
            f"Failed to transform DynamoDB table: {str(e)}",
            table_name=job_config.table_name
        ) from e
