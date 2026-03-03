import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.dev_utils.schema_formatter import print_formatted_schema
from dynamodb_curated_library.etl.transform.utils.align_schema import align_schema

from examples.co_delfos_clientes_prn_transformar_customerknowid_module.schemas.co_dynamodb_customer_know_id_update_module import final_schema
from examples.co_delfos_clientes_prn_transformar_customerknowid_module.transform.utils.cast_timestamp import cast_column_to_date
from examples.co_delfos_clientes_prn_transformar_customerknowid_module.transform.utils.exceptions import TransformationException

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
            on=["customer_id"],
            how="leftanti"
        )
        missing_pk_count = df_missing_pks.count()

        logger.info("Total missing PKs: %d", missing_pk_count)
        if missing_pk_count > 0:
            logger.warning("Found %d records with missing PKs", missing_pk_count)
            df_missing_pks.show(vertical=True, truncate=False, n=missing_pk_count)

        # Add id_reward column as the primary key for the table
        df_with_unique_key = df_flatten.withColumn("id_reward", F.col("customer_id"))

        # Cast timestamp columns
        date_columns = [
            'creation_date',
            'notification_date',
            'update_date'
        ]

        df_with_dates = df_with_unique_key
        for column_name in date_columns:
            df_with_dates = cast_column_to_date(df=df_with_dates, column_name=column_name)

        if create_final_schema:
            print_formatted_schema(df_with_dates.schema)

        df_result = align_schema(df_with_dates, final_schema)
        return df_result

    except Exception as e:
        logger.error("Unexpected error during transformation: %s", str(e))
        raise TransformationException(
            f"Failed to transform DynamoDB table: {str(e)}",
            table_name=job_config.table_name
        ) from e
