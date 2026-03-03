"""Utility functions for column renaming operations."""

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def rename_columns(df: DataFrame, column_mapping: Dict[str, str]) -> DataFrame:
    """
    Rename columns in a DataFrame based on a mapping dictionary.

    Args:
        df: Input DataFrame
        column_mapping: Dictionary mapping old column names to new column names

    Returns:
        DataFrame with renamed columns

    Example:
        >>> mapping = {'old_name': 'new_name', 'another_old': 'another_new'}
        >>> df_renamed = rename_columns(df, mapping)
    """
    select_expressions = [
        F.col(col).alias(column_mapping[col]) if col in column_mapping else F.col(col)
        for col in df.columns
    ]
    return df.select(*select_expressions)
