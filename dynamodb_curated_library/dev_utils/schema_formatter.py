from pyspark.sql.types import StructType, ArrayType


def format_schema_for_display(schema: StructType) -> str:
    """
    Format PySpark schema for readable display.

    Args:
        schema: PySpark StructType schema

    Returns:
        Formatted schema string ready for copy-paste
    """
    def format_field(field, indent_level):
        indent = "    " * indent_level
        if isinstance(field.dataType, StructType):
            lines = [f"{indent}T.StructField('{field.name}', T.StructType(["]
            for sub_field in field.dataType.fields:
                lines.extend(format_field(sub_field, indent_level + 1))
            lines.append(f"{indent}]), {field.nullable}),")
            return lines
        else:
            if isinstance(field.dataType, ArrayType):
                if isinstance(field.dataType.elementType, StructType):
                    # ArrayType with StructType elements - need to format recursively
                    lines = [f"{indent}T.StructField('{field.name}', T.ArrayType("]
                    lines.append(f"{indent}    T.StructType([")
                    for sub_field in field.dataType.elementType.fields:
                        lines.extend(format_field(sub_field, indent_level + 2))
                    lines.append(f"{indent}    ])")
                    lines.append(f"{indent}, {field.dataType.containsNull}), {field.nullable}),")
                    return lines
                else:
                    # ArrayType with simple element type
                    element_type = f"T.{field.dataType.elementType.typeName().title()}Type()"
                    type_name = f"T.ArrayType({element_type}, {field.dataType.containsNull})"
            else:
                type_name = f"T.{field.dataType.typeName().title()}Type()"
            return [f"{indent}T.StructField('{field.name}', {type_name}, {field.nullable}),"]

    lines = ["schema = T.StructType(["]
    for field in schema.fields:
        lines.extend(format_field(field, 1))
    lines.append("])")

    return "\n".join(lines)


def print_formatted_schema(schema: StructType) -> None:
    """
    Print formatted schema with nice presentation.

    Args:
        schema: PySpark StructType schema
    """
    separator = "=" * 80
    formatted_schema = format_schema_for_display(schema)

    print(f"\n{separator}")
    print("FORMATTED SCHEMA FOR COPY-PASTE:")
    print(separator)
    print(formatted_schema)
    print(f"{separator}\n")
