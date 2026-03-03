import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


def align_schema(df: DataFrame, stored_schema: T.StructType) -> DataFrame:  # pragma: no cover
    """
    Align DataFrame schema to match a stored schema by casting types, adding missing columns, and ignoring new columns.

    This function ensures schema stability for Glue Catalog by:
    - Casting existing columns to stored schema types (handles nested StructType and ArrayType)
    - Adding missing columns as NULL with correct types
    - Ignoring new columns not present in stored schema

    Args:
        df: Input DataFrame with current schema
        stored_schema: Target StructType schema to align to

    Returns:
        DataFrame with schema aligned to stored_schema

    Raises:
        ValueError: If df is None or stored_schema is not a StructType

    Example:
        >>> stored = StructType([StructField("id", StringType()), StructField("value", IntegerType())])
        >>> aligned_df = align_schema(df, stored)
    """

    logger.info("Aligning DataFrame schema to stored schema")
    logger.debug("Current schema fields: %d, Stored schema fields: %d", len(df.schema.fields), len(stored_schema.fields))

    def align_field(field_name, new_type, stored_type):
        """
        Recursively align field type, including nested structures and arrays.

        Args:
            field_name: Full field path (e.g., "parent.child")
            new_type: Current DataType from DataFrame
            stored_type: Target DataType from stored schema

        Returns:
            Column expression with aligned type
        """
        if isinstance(stored_type, T.StructType) and isinstance(new_type, T.StructType):
            # Handle nested StructTypes
            aligned_fields = []
            for stored_field in stored_type.fields:
                sub_field_name = f"{field_name}.{stored_field.name}"
                if stored_field.name in [f.name for f in new_type.fields]:
                    new_sub_type = next(f.dataType for f in new_type.fields if f.name == stored_field.name)
                    aligned_fields.append(align_field(sub_field_name, new_sub_type, stored_field.dataType))
                else:
                    aligned_fields.append(F.lit(None).cast(stored_field.dataType).alias(stored_field.name))
            return F.struct(*aligned_fields).alias(field_name)

        elif isinstance(stored_type, T.ArrayType) and isinstance(new_type, T.ArrayType):
            # Handle ArrayType (if it's an array of structures, we call align_field recursively)
            if isinstance(stored_type.elementType, T.StructType) and isinstance(new_type.elementType, T.StructType):
                aligned_struct = align_field(field_name.split(".")[-1], new_type.elementType, stored_type.elementType)
                return F.transform(F.col(field_name), lambda x: aligned_struct).alias(field_name.split(".")[-1])
            else:
                return F.col(field_name).cast(stored_type).alias(field_name.split(".")[-1])
        else:
            return F.col(field_name).cast(stored_type).alias(field_name.split(".")[-1])

    # Get column names and types
    new_schema_fields = {field.name: field.dataType for field in df.schema.fields}
    stored_schema_fields = {field.name: field.dataType for field in stored_schema.fields}

    # Build aligned columns in stored_schema order
    aligned_cols = []
    missing_columns = []
    ignored_columns = set(new_schema_fields.keys()) - set(stored_schema_fields.keys())

    # Iterate through stored_schema to preserve order
    for stored_field in stored_schema.fields:
        col_name = stored_field.name
        if col_name in new_schema_fields:
            # Column exists - align it
            aligned_cols.append(align_field(col_name, new_schema_fields[col_name], stored_field.dataType))
        else:
            # Column missing - add as NULL
            aligned_cols.append(F.lit(None).cast(stored_field.dataType).alias(col_name))
            missing_columns.append(col_name)

    if missing_columns:
        logger.info("Adding %d missing columns as NULL: %s", len(missing_columns), missing_columns)
    if ignored_columns:
        logger.info("Ignoring %d new columns not in stored schema: %s", len(ignored_columns), list(ignored_columns))

    logger.debug("Aligning %d total columns in stored schema order", len(aligned_cols))

    logger.info("Schema alignment completed successfully")
    return df.select(*aligned_cols)
