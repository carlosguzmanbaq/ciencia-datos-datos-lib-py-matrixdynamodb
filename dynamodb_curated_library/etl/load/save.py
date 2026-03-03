import os
from typing import Dict, Any
from pyspark.sql import DataFrame

from dynamodb_curated_library.dev_utils.local_env import IS_LOCAL_ENV

class SaveException(Exception):
    """Exception raised when saving DataFrame fails"""


def save(
    df: DataFrame,
    hudi_options: Dict[str, Any],
    path: str,
    write_format: str = "parquet",
    mode: str = "overwrite",
):
    """
    Save DataFrame to storage in specified format.

    Args:
        df: DataFrame to save
        hudi_options: Hudi configuration options
        path: S3 path where data will be saved
        write_format: Format to save data (parquet, hudi, etc.)
        mode: Write mode (overwrite, append, etc.)

    Raises:
        SaveException: If saving fails
        ValueError: If required parameters are missing
    """
    if df is None:
        raise ValueError("df cannot be None")
    if hudi_options is None:
        raise ValueError("hudi_options cannot be None")
    if path is None:
        raise ValueError("path cannot be None")

    try:
        if IS_LOCAL_ENV:
            _save_local(df, hudi_options)
        else:
            table = hudi_options.get('hoodie.table.name')
            database = hudi_options.get('hoodie.datasource.hive_sync.database')
            print(f'Saving catalog: {database}.{table}')

            df.write.format(write_format).mode(mode).options(**hudi_options).save(path)

    except Exception as error:
        raise SaveException(f"Failed to save DataFrame to {path}: {str(error)}") from error


def _save_local(df: DataFrame, hudi_options: Dict[str, Any]):
    """
    Save DataFrame locally for development environment.

    Args:
        df: DataFrame to save
        hudi_options: Hudi configuration options containing table and database names
    """
    current_path = os.getcwd()
    table = hudi_options.get('hoodie.table.name')
    database = hudi_options.get('hoodie.datasource.hive_sync.database')

    if not table or not database:
        raise ValueError("hudi_options must contain 'hoodie.table.name' and 'hoodie.datasource.hive_sync.database'")

    df.sparkSession.sql(f'CREATE DATABASE IF NOT EXISTS {database}').collect()
    print(f'Saving to local catalog: {database}.{table}')

    (
        df
        .write
        .format('parquet')
        .mode('overwrite')
        .option('path', f"{current_path}/dev/catalog/curated/{table}")
        .saveAsTable(f'{database}.{table}')
    )


def save_hudi(
    df: DataFrame,
    hudi_options: Dict[str, Any],
    path: str,
    mode: str = "overwrite",
):
    """
    Save DataFrame in Hudi format.

    Args:
        df: DataFrame to save
        hudi_options: Hudi configuration options
        path: S3 path where data will be saved
        mode: Write mode (overwrite, append, etc.)

    Raises:
        SaveException: If saving fails
    """
    save(df=df, hudi_options=hudi_options, path=path, write_format="hudi", mode=mode)
