import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.dev_utils.schema_formatter import print_formatted_schema
from dynamodb_curated_library.etl.transform.utils.align_schema import align_schema

from examples.co_delfos_productos_srf_transformar_remittances_order_status.schemas.co_dynamodb_remittances_order_status_schema import final_schema
from examples.co_delfos_productos_srf_transformar_remittances_order_status.transform.utils.cast_timestamp import cast_column_timestamp
from examples.co_delfos_productos_srf_transformar_remittances_order_status.transform.utils.rename_columns import rename_columns
from examples.co_delfos_productos_srf_transformar_remittances_order_status.transform.utils.exceptions import TransformationException

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

        # Rename main columns
        column_mapping = {
            'creation_date_t_stamp': 'creation_date_tstamp',
            'order_bene_document_i_d': 'order_bene_document_id',
            'order_bene_remittances_i_d': 'order_bene_remittances_id',
            'order_date_time_stamp': 'order_date_timestamp',
            'state_date_t_stamp': 'state_date_tstamp',
        }
        df_renamed = rename_columns(df_flatten, column_mapping)

        # Left anti join to validate if there are any PKs not present in the keys DataFrame
        df_missing_pks = df_keys.join(
            df_renamed,
            on=["order_id", "remittance_id"],
            how="leftanti"
        )
        missing_pk_count = df_missing_pks.count()

        logger.info("Total missing PKs: %d", missing_pk_count)
        if missing_pk_count > 0:
            logger.warning("Found %d records with missing PKs", missing_pk_count)
            df_missing_pks.show(vertical=True, truncate=False, n=missing_pk_count)

        # Add id_reward column as the primary key for the table
        df_with_unique_key = df_renamed.withColumn("id_reward", F.concat_ws("_", F.col("order_id"), F.col("remittance_id")))

        # Cast timestamp columns
        timestamp_columns = [
            "creation_date_tstamp",
            "customer_fecha_expedicion",
            "customer_fecha_nacimiento",
            "order_date",
            "order_date_timestamp",
            "order_sales_date",
            "state_date",
            "state_date_tstamp"
        ]

        df_with_timestamps = df_with_unique_key
        for column_name in timestamp_columns:
            df_with_timestamps = cast_column_timestamp(df=df_with_timestamps, column_name=column_name)

        if create_final_schema:
            print_formatted_schema(df_with_timestamps.schema)

        df_result = align_schema(df_with_timestamps, final_schema)
        return df_result

    except Exception as e:
        logger.error("Unexpected error during transformation: %s", str(e))
        raise TransformationException(
            f"Failed to transform DynamoDB table: {str(e)}",
            table_name=job_config.table_name
        ) from e
