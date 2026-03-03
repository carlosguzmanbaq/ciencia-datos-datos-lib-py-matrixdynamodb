"""
Tests for schema_formatter module.

Tests cover:
- Simple types formatting
- Nested StructType formatting
- ArrayType with simple elements
- ArrayType with StructType elements
- Nullable and non-nullable fields
- Multiple nesting levels
- Print function output
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
    LongType,
    DoubleType,
)


from dynamodb_curated_library.dev_utils.schema_formatter import (
    format_schema_for_display,
    print_formatted_schema,
)


class TestFormatSchemaSimpleTypes:
    """Test formatting of simple data types."""

    def test_single_string_field(self):
        """Test formatting schema with single string field."""
        schema = StructType([StructField("name", StringType(), True)])

        result = format_schema_for_display(schema)

        assert "schema = T.StructType([" in result
        assert "T.StructField('name', T.StringType(), True)," in result
        assert "])" in result

    def test_multiple_simple_fields(self):
        """Test formatting schema with multiple simple fields."""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("active", BooleanType(), True),
            ]
        )

        result = format_schema_for_display(schema)

        assert "T.StructField('id', T.IntegerType(), False)," in result
        assert "T.StructField('name', T.StringType(), True)," in result
        assert "T.StructField('active', T.BooleanType(), True)," in result

    def test_various_numeric_types(self):
        """Test formatting schema with various numeric types."""
        schema = StructType(
            [
                StructField("int_field", IntegerType(), True),
                StructField("long_field", LongType(), True),
                StructField("double_field", DoubleType(), True),
            ]
        )

        result = format_schema_for_display(schema)

        assert "T.IntegerType()" in result
        assert "T.LongType()" in result
        assert "T.DoubleType()" in result


class TestFormatSchemaNestedStructs:
    """Test formatting of nested StructType fields."""

    def test_single_level_nested_struct(self):
        """Test formatting schema with one level of nesting."""
        schema = StructType(
            [
                StructField(
                    "address",
                    StructType(
                        [
                            StructField("street", StringType(), True),
                            StructField("city", StringType(), True),
                        ]
                    ),
                    True,
                )
            ]
        )

        result = format_schema_for_display(schema)

        assert "T.StructField('address', T.StructType([" in result
        assert "T.StructField('street', T.StringType(), True)," in result
        assert "T.StructField('city', T.StringType(), True)," in result
        assert "]), True)," in result

    def test_multiple_nested_levels(self):
        """Test formatting schema with multiple nesting levels."""
        schema = StructType(
            [
                StructField(
                    "user",
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField(
                                "address",
                                StructType(
                                    [
                                        StructField("street", StringType(), True),
                                        StructField(
                                            "location",
                                            StructType(
                                                [
                                                    StructField("lat", DoubleType(), True),
                                                    StructField("lon", DoubleType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                )
            ]
        )

        result = format_schema_for_display(schema)

        assert "T.StructField('user', T.StructType([" in result
        assert "T.StructField('address', T.StructType([" in result
        assert "T.StructField('location', T.StructType([" in result
        assert "T.StructField('lat', T.DoubleType(), True)," in result


class TestFormatSchemaArrayTypes:
    """Test formatting of ArrayType fields."""

    def test_array_with_simple_type(self):
        """Test formatting array with simple element type."""
        schema = StructType([StructField("tags", ArrayType(StringType(), True), True)])

        result = format_schema_for_display(schema)

        assert "T.StructField('tags', T.ArrayType(T.StringType(), True), True)," in result

    def test_array_with_non_nullable_elements(self):
        """Test formatting array with non-nullable elements."""
        schema = StructType([StructField("ids", ArrayType(IntegerType(), False), True)])

        result = format_schema_for_display(schema)

        assert "T.ArrayType(T.IntegerType(), False)" in result

    def test_array_with_struct_elements(self):
        """Test formatting array with StructType elements."""
        schema = StructType(
            [
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                )
            ]
        )

        result = format_schema_for_display(schema)

        assert "T.StructField('items', T.ArrayType(" in result
        assert "T.StructType([" in result
        assert "T.StructField('id', T.IntegerType(), True)," in result
        assert "T.StructField('name', T.StringType(), True)," in result

    def test_array_with_nested_struct_elements(self):
        """Test formatting array with nested StructType elements."""
        schema = StructType(
            [
                StructField(
                    "orders",
                    ArrayType(
                        StructType(
                            [
                                StructField("order_id", StringType(), True),
                                StructField(
                                    "customer",
                                    StructType(
                                        [
                                            StructField("name", StringType(), True),
                                            StructField("email", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                    True,
                )
            ]
        )

        result = format_schema_for_display(schema)

        assert "T.StructField('orders', T.ArrayType(" in result
        assert "T.StructField('order_id', T.StringType(), True)," in result
        assert "T.StructField('customer', T.StructType([" in result
        assert "T.StructField('name', T.StringType(), True)," in result


class TestFormatSchemaNullability:
    """Test formatting of nullable and non-nullable fields."""

    def test_non_nullable_field(self):
        """Test formatting non-nullable field."""
        schema = StructType([StructField("id", IntegerType(), False)])

        result = format_schema_for_display(schema)

        assert "T.StructField('id', T.IntegerType(), False)," in result

    def test_nullable_field(self):
        """Test formatting nullable field."""
        schema = StructType([StructField("name", StringType(), True)])

        result = format_schema_for_display(schema)

        assert "T.StructField('name', T.StringType(), True)," in result

    def test_mixed_nullability(self):
        """Test formatting schema with mixed nullability."""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("email", StringType(), False),
            ]
        )

        result = format_schema_for_display(schema)

        assert "T.StructField('id', T.IntegerType(), False)," in result
        assert "T.StructField('name', T.StringType(), True)," in result
        assert "T.StructField('email', T.StringType(), False)," in result


class TestFormatSchemaIndentation:
    """Test proper indentation in formatted output."""

    def test_nested_struct_indentation(self):
        """Test indentation for nested structs."""
        schema = StructType(
            [
                StructField(
                    "level1",
                    StructType(
                        [
                            StructField(
                                "level2",
                                StructType([StructField("level3", StringType(), True)]),
                                True,
                            )
                        ]
                    ),
                    True,
                )
            ]
        )

        result = format_schema_for_display(schema)
        lines = result.split("\n")

        # Check indentation levels (4 spaces per level)
        assert any("    T.StructField('level1'" in line for line in lines)
        assert any("        T.StructField('level2'" in line for line in lines)
        assert any("            T.StructField('level3'" in line for line in lines)

    def test_array_struct_indentation(self):
        """Test indentation for array with struct elements."""
        schema = StructType(
            [
                StructField(
                    "items",
                    ArrayType(
                        StructType([StructField("name", StringType(), True)]), True
                    ),
                    True,
                )
            ]
        )

        result = format_schema_for_display(schema)
        lines = result.split("\n")

        # Check proper indentation for array elements
        assert any("    T.StructField('items', T.ArrayType(" in line for line in lines)
        assert any("        T.StructType([" in line for line in lines)
        assert any("            T.StructField('name'" in line for line in lines)


class TestPrintFormattedSchema:
    """Test print_formatted_schema function."""

    def test_print_output_format(self, capsys):
        """Test that print function outputs correct format."""
        schema = StructType([StructField("name", StringType(), True)])

        print_formatted_schema(schema)
        captured = capsys.readouterr()

        assert "=" * 80 in captured.out
        assert "FORMATTED SCHEMA FOR COPY-PASTE:" in captured.out
        assert "schema = T.StructType([" in captured.out
        assert "T.StructField('name', T.StringType(), True)," in captured.out

    def test_print_with_complex_schema(self, capsys):
        """Test print function with complex schema."""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField(
                    "data",
                    StructType([StructField("value", StringType(), True)]),
                    True,
                ),
            ]
        )

        print_formatted_schema(schema)
        captured = capsys.readouterr()

        assert "T.StructField('id', T.IntegerType(), False)," in captured.out
        assert "T.StructField('data', T.StructType([" in captured.out
        assert "T.StructField('value', T.StringType(), True)," in captured.out


class TestFormatSchemaEdgeCases:
    """Test edge cases and special scenarios."""

    def test_empty_schema(self):
        """Test formatting empty schema."""
        schema = StructType([])

        result = format_schema_for_display(schema)

        assert result == "schema = T.StructType([\n])"

    def test_empty_nested_struct(self):
        """Test formatting schema with empty nested struct."""
        schema = StructType([StructField("empty", StructType([]), True)])

        result = format_schema_for_display(schema)

        assert "T.StructField('empty', T.StructType([" in result
        assert "]), True)," in result

    def test_complex_real_world_schema(self):
        """Test formatting complex real-world-like schema."""
        schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("product_id", StringType(), True),
                                StructField("quantity", IntegerType(), True),
                                StructField("price", DoubleType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
                StructField(
                    "shipping_address",
                    StructType(
                        [
                            StructField("street", StringType(), True),
                            StructField("city", StringType(), True),
                            StructField(
                                "coordinates",
                                StructType(
                                    [
                                        StructField("lat", DoubleType(), True),
                                        StructField("lon", DoubleType(), True),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                StructField("created_at", StringType(), True),
            ]
        )

        result = format_schema_for_display(schema)

        # Verify all fields are present
        assert "order_id" in result
        assert "customer_id" in result
        assert "items" in result
        assert "shipping_address" in result
        assert "coordinates" in result
        assert "created_at" in result

        # Verify structure
        assert "T.ArrayType(" in result
        assert "T.StructType([" in result
        assert result.startswith("schema = T.StructType([")
        assert result.endswith("])")
