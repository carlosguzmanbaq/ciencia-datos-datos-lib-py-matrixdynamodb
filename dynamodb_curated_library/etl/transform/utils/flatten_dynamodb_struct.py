import re
import logging
from typing import List, Optional
from pyspark.sql import DataFrame, functions as F, types as T

from dynamodb_curated_library.etl.transform.utils.exceptions import FlattenStructureException

logger = logging.getLogger(__name__)


DYNAMODB_TYPES = {"M", "L", "S", "N", "BOOL", "NULL"}
DYNAMODB_VALUE_TYPES = ["S", "BOOL", "N"]

# Priority order for conflicting DynamoDB types (higher index = higher priority)
# For structural types only - value types use coalesce
TYPE_PRIORITY = {"NULL": 0, "BOOL": 1, "N": 2, "S": 3, "M": 4, "L": 5}


def to_snake_case(name: str) -> str:
    """
    Convert lowerCamelCase string to snake_case.

    Args:
        name: String in camelCase format

    Returns:
        String in snake_case format
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def calculate_depth(path: str) -> int:
    """
    Calculate the depth level of a path, excluding DynamoDB type markers.

    Args:
        path: Full path string (e.g., "state.M.orderSteps.M.items")

    Returns:
        Depth level (e.g., "state.M.orderSteps" = depth 2)
    """
    parts = [p for p in path.split(".") if p not in DYNAMODB_TYPES]
    return len(parts)


def extract_fields_from_array(schema, array_path: str):
    """
    Extract fields from ArrayType(StructType) structure, handling DynamoDB 'M' wrapper.
    Uses coalesce for fields with multiple types and casts everything to string.

    Args:
        schema: PySpark DataFrame schema
        array_path: Path to array column in schema

    Returns:
        Tuple of (has_m_wrapper: bool, fields: List[tuples])
        where fields are (snake_case_name, original_name, coalesce_expr)
    """
    parts = array_path.split(".")
    current_schema = schema

    for part in parts:
        field = next((f for f in current_schema.fields if f.name == part), None)
        if field is None:
            return False, []  # Path does not exist in schema

        # If it's an array (ArrayType)
        if isinstance(field.dataType, T.ArrayType):
            element_type = field.dataType.elementType

            # If array elements are structures (StructType)
            if isinstance(element_type, T.StructType):
                # Look for 'M' key inside StructType
                m_field = next((f for f in element_type.fields if f.name == "M"), None)

                if m_field and isinstance(m_field.dataType, T.StructType):
                    # Has M wrapper
                    extracted_fields = []
                    for subfield in m_field.dataType.fields:
                        field_name_snake = to_snake_case(subfield.name)

                        if isinstance(subfield.dataType, T.StructType):
                            # Get available value types and create coalesce expression
                            available_types = [f.name for f in subfield.dataType.fields if f.name in DYNAMODB_VALUE_TYPES]
                            if available_types:
                                # Create coalesce expression for multiple types
                                coalesce_parts = [f"x.M.{subfield.name}.{t}" for t in available_types]
                                coalesce_expr = f"cast(coalesce({', '.join(coalesce_parts)}) as string)"
                                extracted_fields.append((field_name_snake, subfield.name, coalesce_expr))
                        else:
                            extracted_fields.append((field_name_snake, subfield.name, f"cast(x.M.{subfield.name} as string)"))
                    return True, extracted_fields

                else:
                    # No M wrapper - direct StructType fields
                    fields = []
                    for subfield in element_type.fields:
                        field_name_snake = to_snake_case(subfield.name)

                        if isinstance(subfield.dataType, T.StructType):
                            available_types = [f.name for f in subfield.dataType.fields if f.name in DYNAMODB_VALUE_TYPES]
                            if available_types:
                                coalesce_parts = [f"x.{subfield.name}.{t}" for t in available_types]
                                coalesce_expr = f"cast(coalesce({', '.join(coalesce_parts)}) as string)"
                                fields.append((field_name_snake, subfield.name, coalesce_expr))
                        else:
                            fields.append((field_name_snake, subfield.name, f"cast(x.{subfield.name} as string)"))
                    return False, fields

            return False, []  # If not StructType inside Array, nothing to extract

        elif isinstance(field.dataType, T.StructType):
            current_schema = field.dataType  # Continue exploring

        else:
            return False, []  # Not a struct or array, end search

    return False, []


def create_coalesce_expression(full_path: str, struct_fields, alias_name: str):
    """
    Create coalesce expression for DynamoDB value types, preserving all non-null values.

    Args:
        full_path: Path to the struct field
        struct_fields: List of StructField objects
        alias_name: Alias name for the resulting column

    Returns:
        Column expression with coalesce of all value types, cast to string
    """
    available_value_types = [f.name for f in struct_fields if f.name in DYNAMODB_VALUE_TYPES]

    if not available_value_types:
        return None

    # Create coalesce expression for all available value types
    columns = [
        F.col(f"{full_path}.{data_type}").cast(T.StringType())
        for data_type in ["S", "N", "BOOL"] if data_type in available_value_types
    ]

    if columns:
        return F.coalesce(*columns).alias(alias_name)
    return None


def get_highest_priority_type(struct_fields) -> Optional[str]:
    """
    Get the DynamoDB type with highest priority from a struct's fields.

    Args:
        struct_fields: List of StructField objects

    Returns:
        String of highest priority DynamoDB type, or None if no DynamoDB types found
    """
    available_types = [f.name for f in struct_fields if f.name in DYNAMODB_TYPES]
    if not available_types:
        return None

    # Return type with highest priority
    return max(available_types, key=lambda t: TYPE_PRIORITY.get(t, -1))


def flatten_dynamodb_struct(
    df: DataFrame,
    max_depth: int,
    columns_as_json: Optional[List[str]],
    parent_col: str = "NewImage",
) -> DataFrame:
    """
    Flatten nested DynamoDB export structure into normalized DataFrame.

    Args:
        df: PySpark DataFrame with nested DynamoDB structure
        parent_col: Root column name containing data (default: 'NewImage')
        max_depth: Maximum depth level to flatten (default: 3). Deeper structures will be converted to JSON string
        columns_as_json: List of column paths to keep as JSON string instead of flattening
                        Example: ["state.M.orderSteps", "metadata.M.details"]

    Returns:
        DataFrame with flattened DynamoDB structure

    Raises:
        Exception: If flattening fails, suggests which columns to add to columns_as_json
    """
    df = df.select(F.col(f"{parent_col}.*"))

    flat_cols = []
    array_cols = []
    columns_as_json = columns_as_json or []

    def should_convert_to_json(full_path: str, current_depth: int) -> bool:
        """Check if a path should be converted to JSON string."""
        # Check if path is in user-specified list
        for json_path in columns_as_json:
            if full_path.startswith(json_path):
                return True

        # Check if depth exceeds max_depth
        if current_depth > max_depth:
            return True

        return False

    def recurse_schema(schema, prefix: str = ""):
        for field in schema.fields:
            field_name = field.name
            full_path = f"{prefix}.{field_name}" if prefix else field_name
            full_path_parts = full_path.split(".")
            alias_name = '_'.join([part for part in full_path_parts if part not in DYNAMODB_TYPES])
            alias_name = to_snake_case(alias_name).strip("_")

            # Calculate actual depth (excluding DynamoDB type markers)
            actual_depth = calculate_depth(full_path)

            # if full_path == 'state.M.orderSteps.NULL':
            #     continue

            # Convert to JSON if depth exceeded or user specified
            if should_convert_to_json(full_path, actual_depth):
                logger.debug("Converting to JSON (depth %d): %s", actual_depth, full_path)
                flat_cols.append(F.to_json(F.col(full_path)).alias(alias_name))
                continue

            if isinstance(field.dataType, T.StructType):
                # Check for conflicting DynamoDB types and resolve
                highest_priority_type = get_highest_priority_type(field.dataType.fields)
                available_types = [f.name for f in field.dataType.fields if f.name in DYNAMODB_TYPES]

                # Log conflicts if multiple DynamoDB types exist
                if len(available_types) > 1:
                    logger.debug(
                        "Conflict detected for %s: %s. Using highest priority: %s",
                        full_path,
                        available_types,
                        highest_priority_type
                    )

                # Handle based on highest priority type
                if highest_priority_type == "L":  # Array type has highest priority
                    array_field = next(f for f in field.dataType.fields if f.name == "L")
                    if isinstance(array_field.dataType, T.ArrayType) and isinstance(array_field.dataType.elementType, T.StructType):
                        array_cols.append((full_path + ".L", alias_name))
                    else:
                        flat_cols.append(F.col(full_path + ".L").alias(alias_name))
                    continue

                elif highest_priority_type == "M":  # Map type
                    # Check depth before recursing into nested structures
                    if actual_depth >= max_depth:
                        logger.debug("Converting to JSON (depth %d >= max %d): %s", actual_depth, max_depth, full_path)
                        flat_cols.append(F.to_json(F.col(full_path + ".M")).alias(alias_name))
                    else:
                        recurse_schema(next(f for f in field.dataType.fields if f.name == "M").dataType, full_path + ".M")
                    continue

                # For value types (S, N, BOOL, NULL), use coalesce to preserve all values
                coalesce_expr = create_coalesce_expression(full_path, field.dataType.fields, alias_name)
                if coalesce_expr is not None:
                    flat_cols.append(coalesce_expr)
                    continue

                # Check if StructType contains nested StructType or ArrayType (for non-priority types)
                contains_struct = any(isinstance(subfield.dataType, T.StructType) for subfield in field.dataType.fields)
                contains_array = any(isinstance(subfield.dataType, T.ArrayType) for subfield in field.dataType.fields)

                if contains_struct or contains_array:
                    # Check depth before recursing into nested structures
                    if actual_depth >= max_depth:
                        logger.debug("Converting to JSON (depth %d >= max %d): %s", actual_depth, max_depth, full_path)
                        flat_cols.append(F.to_json(F.col(full_path)).alias(alias_name))
                    else:
                        recurse_schema(field.dataType, full_path)

            elif isinstance(field.dataType, T.ArrayType):
                if isinstance(field.dataType.elementType, T.StructType):
                    array_cols.append((full_path, alias_name))
                else:
                    flat_cols.append(F.col(full_path).alias(alias_name))

            else:
                flat_cols.append(F.col(full_path).cast(T.StringType()).alias(alias_name))

    recurse_schema(df.schema, "")

    # Apply dynamic transformation to structured lists
    for array_path, alias_name in array_cols:
        has_m_wrapper, extracted_fields = extract_fields_from_array(df.schema, array_path)
        if extracted_fields:
            logger.debug("Processing array - path: %s, has_M_wrapper: %s", array_path, has_m_wrapper)

            # Generate struct fields with coalesce expressions
            struct_fields = ", ".join([
                f"'{snake_name}', {coalesce_expr}"
                for snake_name, _, coalesce_expr in extracted_fields
            ])

            transform_expr = F.expr(f"TRANSFORM({array_path}, x -> named_struct({struct_fields}))")
            flat_cols.append(transform_expr.alias(alias_name))

    # Try to select with error handling
    try:
        return df.select(*flat_cols)
    except Exception as e:
        raise FlattenStructureException(e, max_depth, parent_col) from e
