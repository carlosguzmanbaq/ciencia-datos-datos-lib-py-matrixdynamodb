import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.extract.catalog import Catalog
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import to_snake_case

logger = logging.getLogger(__name__)


class CatalogNotFoundException(Exception):
    """Exception raised when catalog table does not exist for INC process with event_status enabled."""


def add_event_status_flag(
    df_flatten: DataFrame,
    df_keys: DataFrame,
    catalog: Catalog,
    job_config: JobConfig
) -> DataFrame:
    """
    Add flag_event_status column based on DynamoDB operation type.

    Logic:
        - FULL: All records are INSERT (initial load)
        - INC:
            - has_new_image=True + PK exists in catalog → UPDATE
            - has_new_image=True + PK NOT exists in catalog → INSERT
            - has_new_image=False + PK exists in catalog → DELETE

    Args:
        df_flatten: Flattened DataFrame with transformed data
        df_keys: DataFrame with PKs and has_new_image flag
        catalog: Catalog instance to read existing data
        job_config: Job configuration

    Returns:
        DataFrame with flag_event_status column added

    Raises:
        CatalogNotFoundException: If catalog table doesn't exist for INC with event_status enabled
    """
    primary_keys_snake = [to_snake_case(pk) for pk in job_config.constants.primary_keys]

    if job_config.process_type == "FULL":
        logger.info("FULL process: Setting all records as INSERT")
        return df_flatten.withColumn("flag_event_status", F.lit("INSERTED"))

    # INC process: Read existing PKs from catalog
    logger.info("INC process: Reading existing PKs from catalog")
    try:
        df_existing = catalog.get_dataframe_from_catalog(
            job_config=job_config,
            select_columns=primary_keys_snake
        ).withColumn("pk_exists", F.lit(True))

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Existing PKs count: %d", df_existing.count())
    except Exception as e:
        error_msg = (
            f"Catalog table does not exist: {job_config.table_catalog_names.curated_database_name}."
            f"{job_config.table_catalog_names.curated_table_name}. "
            f"For INC process with event_status enabled, you must run FULL load first to create the table. "
            f"Original error: {str(e)}"
        )
        logger.error(error_msg)
        raise CatalogNotFoundException(error_msg) from e

    # Join with keys and existing PKs
    df_result = (
        df_keys
        .join(df_flatten, on=primary_keys_snake, how="left")
        .join(df_existing, on=primary_keys_snake, how="left")
        .withColumn(
            "flag_event_status",
            F.when(~F.col("has_new_image") & F.col("pk_exists"), F.lit("DELETED"))
            .when(F.col("has_new_image") & F.col("pk_exists"), F.lit("UPDATED"))
            .when(F.col("has_new_image") & F.col("pk_exists").isNull(), F.lit("INSERTED"))
            .otherwise(F.lit("UNKNOWN"))
        )
        .drop("has_new_image", "pk_exists")
    )

    return df_result


def enrich_deleted_records(
    df_with_status: DataFrame,
    catalog: Catalog,
    job_config: JobConfig
) -> DataFrame:
    """
    Enrich DELETED records with data from catalog snapshot.

    For DELETE operations, recovers full record data from catalog
    instead of keeping only primary keys.

    Args:
        df_with_status: DataFrame with flag_event_status column
        catalog: Catalog instance to read existing data
        job_config: Job configuration

    Returns:
        DataFrame with DELETED records enriched with catalog data
    """
    primary_keys_snake = [to_snake_case(pk) for pk in job_config.constants.primary_keys]

    # Guard clause: Skip if no DELETE records
    if df_with_status.filter(F.col("flag_event_status") == "DELETED").limit(1).count() == 0:
        logger.info("No DELETE records found. Skipping enrichment.")
        return df_with_status

    logger.info("DELETE records detected. Enriching from catalog snapshot.")

    # Read full snapshot from catalog
    df_existing = catalog.get_dataframe_from_catalog(job_config=job_config)

    # Identify columns to enrich (exclude PKs and technical columns)
    technical_columns = {"flag_event_status"}
    business_columns = [
        c for c in df_with_status.columns
        if c not in primary_keys_snake and c not in technical_columns
    ]

    # Build coalesce expressions for all business columns at once
    select_expr = (
        [F.col(f"df_with_status.{pk}").alias(pk) for pk in primary_keys_snake] +
        [
            F.when(
                F.col("df_with_status.flag_event_status") == "DELETED",
                F.col(f"existing.{col}")
            ).otherwise(F.col(f"df_with_status.{col}")).alias(col)
            for col in business_columns
        ] +
        [F.col("df_with_status.flag_event_status").alias("flag_event_status")]
    )

    # Single join and select operation
    df_result = (
        df_with_status.alias("df_with_status")
        .join(
            df_existing.alias("existing"),
            on=primary_keys_snake,
            how="left"
        )
        .select(*select_expr)
    )

    return df_result
