from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def extract_dynamodb_value(df: DataFrame, col_path: str, alias: str):
    """
    Extract value from DynamoDB field regardless of its data type.

    This function navigates through the DataFrame schema to locate a specific field
    and extracts its value based on the DynamoDB type wrapper (S, N, BOOL, B).
    It handles the DynamoDB export format where each field is wrapped with its
    corresponding type indicator.

    Args:
        df (DataFrame): The PySpark DataFrame containing DynamoDB export data
        col_path (str): Dot-separated path to the field (e.g., "Item.orderId" or "Keys.userId")
        alias (str): The alias name for the extracted column in the result

    Returns:
        Column: A PySpark Column expression with the extracted value and specified alias.
                Returns F.lit(None) if the field doesn't exist or has an unsupported type.

    Supported DynamoDB Types:
        - S: String values
        - N: Numeric values (returned as string)
        - BOOL: Boolean values (cast to string)
        - B: Binary values

    Example:
        >>> # Extract orderId from Item.orderId.S
        >>> col_expr = extract_dynamodb_value(df, "Item.orderId", "order_id")
        >>> result_df = df.select(col_expr)

        >>> # Extract userId from Keys.userId.N
        >>> col_expr = extract_dynamodb_value(df, "Keys.userId", "user_id")
        >>> result_df = df.select(col_expr)
    """
    parts = col_path.split('.')
    current_type = df.schema

    # Navigate through schema to find the field
    for part in parts:
        if not isinstance(current_type, StructType):
            return F.lit(None).alias(alias)

        field = next((f for f in current_type.fields if f.name == part), None)
        if field is None:
            return F.lit(None).alias(alias)

        current_type = field.dataType

    # Now check which DynamoDB type exists in this specific field
    if isinstance(current_type, StructType):
        field_names = {f.name for f in current_type.fields}

        if 'S' in field_names:
            return F.col(f"{col_path}.S").alias(alias)
        if 'N' in field_names:
            return F.col(f"{col_path}.N").alias(alias)
        if 'BOOL' in field_names:
            return F.col(f"{col_path}.BOOL").cast("string").alias(alias)
        if 'B' in field_names:
            return F.col(f"{col_path}.B").alias(alias)

    return F.lit(None).alias(alias)
