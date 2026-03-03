"""
Tests for handling conflicting DynamoDB types in the same field.

This module tests scenarios where a field has multiple DynamoDB type representations
(e.g., notifications.L and notifications.NULL) which would create column name conflicts.
"""

from pyspark.sql import types as T
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import flatten_dynamodb_struct


class TestConflictingTypes:
    """Test handling of conflicting DynamoDB types for the same field name."""

    def test_array_vs_null_conflict_prioritizes_array(self, spark):
        """
        Test that when a field has both L (array) and NULL types,
        the array type takes priority and NULL is ignored.
        """
        # Schema with conflicting notifications field
        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("customerId", T.StructType([
                    T.StructField("S", T.StringType(), True)
                ]), True),
                T.StructField("notifications", T.StructType([
                    T.StructField("L", T.ArrayType(T.StructType([
                        T.StructField("M", T.StructType([
                            T.StructField("messageId", T.StructType([
                                T.StructField("S", T.StringType(), True)
                            ]), True),
                            T.StructField("type", T.StructType([
                                T.StructField("S", T.StringType(), True)
                            ]), True)
                        ]), True)
                    ])), True),
                    T.StructField("NULL", T.BooleanType(), True)
                ]), True)
            ]), True)
        ])

        # Test data with both array and null
        data = [
            {
                "Item": {
                    "customerId": {"S": "123"},
                    "notifications": {
                        "L": [
                            {"M": {"messageId": {"S": "msg1"}, "type": {"S": "email"}}},
                            {"M": {"messageId": {"S": "msg2"}, "type": {"S": "sms"}}}
                        ],
                        "NULL": True
                    }
                }
            }
        ]

        df = spark.createDataFrame(data, schema)

        result = flatten_dynamodb_struct(
            df=df,
            parent_col="Item",
            max_depth=3,
            columns_as_json=None
        )

        # Should have array-based notifications, not NULL
        columns = result.columns
        assert "notifications" in columns
        assert "customer_id" in columns

        # Verify notifications is array type
        notifications_field = next(f for f in result.schema.fields if f.name == "notifications")
        assert isinstance(notifications_field.dataType, T.ArrayType)

    def test_string_vs_bool_conflict_uses_coalesce(self, spark):
        """
        Test that when a field has both S (string) and BOOL types,
        coalesce is used to preserve all non-null values.
        """
        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("status", T.StructType([
                    T.StructField("S", T.StringType(), True),
                    T.StructField("BOOL", T.BooleanType(), True)
                ]), True)
            ]), True)
        ])

        data = [
            # S has value, BOOL is null - should get S
            {
                "Item": {
                    "status": {
                        "S": "active",
                        "BOOL": None
                    }
                }
            },
            # BOOL has value, S is null - should get BOOL
            {
                "Item": {
                    "status": {
                        "S": None,
                        "BOOL": True
                    }
                }
            }
        ]

        df = spark.createDataFrame(data, schema)

        result = flatten_dynamodb_struct(
            df=df,
            parent_col="Item",
            max_depth=3,
            columns_as_json=None
        )

        rows = result.collect()
        # Should preserve both values using coalesce
        assert rows[0]["status"] == "active"  # S value
        assert rows[1]["status"] == "true"    # BOOL value converted to string

    def test_multiple_conflicts_with_mixed_strategy(self, spark):
        """
        Test mixed strategy: L/M use priority, value types use coalesce
        """
        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("field1", T.StructType([
                    T.StructField("L", T.ArrayType(T.StringType()), True),
                    T.StructField("S", T.StringType(), True),
                    T.StructField("NULL", T.BooleanType(), True)
                ]), True),
                T.StructField("field2", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("nested", T.StructType([
                            T.StructField("S", T.StringType(), True)
                        ]), True)
                    ]), True),
                    T.StructField("S", T.StringType(), True)
                ]), True),
                T.StructField("field3", T.StructType([
                    T.StructField("S", T.StringType(), True),
                    T.StructField("BOOL", T.BooleanType(), True)
                ]), True)
            ]), True)
        ])

        data = [
            {
                "Item": {
                    "field1": {
                        "L": ["item1", "item2"],
                        "S": "string_value",
                        "NULL": True
                    },
                    "field2": {
                        "M": {"nested": {"S": "nested_value"}},
                        "S": "simple_string"
                    },
                    "field3": {
                        "S": None,
                        "BOOL": True
                    }
                }
            }
        ]

        df = spark.createDataFrame(data, schema)

        result = flatten_dynamodb_struct(
            df=df,
            parent_col="Item",
            max_depth=3,
            columns_as_json=None
        )

        # field1 should be array (L priority)
        field1_type = next(f for f in result.schema.fields if f.name == "field1").dataType
        assert isinstance(field1_type, T.ArrayType)

        # field2 should be struct (M priority) - will be flattened to field2_nested
        columns = result.columns
        assert "field2_nested" in columns

        # field3 should use coalesce (BOOL value since S is null)
        row = result.collect()[0]
        assert row["field3"] == "true"

    def test_conflict_with_columns_as_json_override(self, spark):
        """
        Test that columns_as_json parameter can override conflict resolution.
        """
        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("notifications", T.StructType([
                    T.StructField("L", T.ArrayType(T.StructType([
                        T.StructField("M", T.StructType([
                            T.StructField("messageId", T.StructType([
                                T.StructField("S", T.StringType(), True)
                            ]), True)
                        ]), True)
                    ])), True),
                    T.StructField("NULL", T.BooleanType(), True)
                ]), True)
            ]), True)
        ])

        data = [
            {
                "Item": {
                    "notifications": {
                        "L": [{"M": {"messageId": {"S": "msg1"}}}],
                        "NULL": True
                    }
                }
            }
        ]

        df = spark.createDataFrame(data, schema)

        result = flatten_dynamodb_struct(
            df=df,
            parent_col="Item",
            max_depth=3,
            columns_as_json=["notifications"]
        )

        # Should be converted to JSON string regardless of conflict
        row = result.collect()[0]
        assert isinstance(row["notifications"], str)
        assert "messageId" in row["notifications"]
