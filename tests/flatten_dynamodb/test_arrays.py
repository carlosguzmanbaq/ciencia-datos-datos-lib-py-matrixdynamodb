"""Arrays tests."""

from pyspark.sql import types as T

from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import (
    flatten_dynamodb_struct
)


class TestArraysWithoutMWrapper:
    """Test arrays that don't have DynamoDB 'M' wrapper."""

    def test_array_without_m_wrapper(self, spark):
        """Test flattening arrays without 'M' wrapper (direct StructType elements)."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "items": {
                        "L": [
                            {"id": {"S": "item1"}, "name": {"S": "Product 1"}},
                            {"id": {"S": "item2"}, "name": {"S": "Product 2"}}
                        ]
                    }
                }
            }
        ]

        # Array elements are StructType directly (no M wrapper)
        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("items", T.StructType([
                    T.StructField("L", T.ArrayType(
                        T.StructType([
                            T.StructField("id", T.StructType([T.StructField("S", T.StringType())])),
                            T.StructField("name", T.StructType([T.StructField("S", T.StringType())]))
                        ])
                    ))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        # Should process array without M wrapper
        assert result.count() == 1
        assert "order_id" in result.columns
        assert "items" in result.columns

        row = result.first()
        assert row.order_id == "123"
        # items should be an array of structs
        assert row.items is not None

    def test_array_with_mixed_field_types(self, spark):
        """Test array elements with fields that have StructType (with type info)."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "tags": {
                        "L": [
                            {"key": {"S": "color"}, "value": {"S": "red"}},
                            {"key": {"S": "size"}, "value": {"N": "42"}}
                        ]
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("tags", T.StructType([
                    T.StructField("L", T.ArrayType(
                        T.StructType([
                            T.StructField("key", T.StructType([T.StructField("S", T.StringType())])),
                            T.StructField("value", T.StructType([
                                T.StructField("S", T.StringType(), nullable=True),
                                T.StructField("N", T.StringType(), nullable=True)
                            ]))
                        ])
                    ))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        assert result.count() == 1
        assert "order_id" in result.columns
        assert "tags" in result.columns

    def test_array_with_simple_types(self, spark):
        """Test array with simple types (not StructType elements)."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "simpleList": {
                        "L": ["value1", "value2", "value3"]
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("simpleList", T.StructType([
                    T.StructField("L", T.ArrayType(T.StringType()))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        # Simple arrays should be kept as-is
        assert result.count() == 1
        assert "order_id" in result.columns
        assert "simple_list" in result.columns

        row = result.first()
        assert row.order_id == "123"
        assert row.simple_list == ["value1", "value2", "value3"]

    def test_empty_array(self, spark):
        """Test handling of empty arrays."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "emptyList": {
                        "L": []
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("emptyList", T.StructType([
                    T.StructField("L", T.ArrayType(T.StringType()))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        assert result.count() == 1
        assert "order_id" in result.columns
        assert "empty_list" in result.columns

        row = result.first()
        assert row.order_id == "123"
        assert row.empty_list == []

    def test_array_elements_with_no_fields(self, spark):
        """Test array with StructType elements that have no fields."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "emptyStructs": {
                        "L": [
                            {},
                            {}
                        ]
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("emptyStructs", T.StructType([
                    T.StructField("L", T.ArrayType(T.StructType([])))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        # Should handle empty structs gracefully
        assert result.count() == 1
        assert "order_id" in result.columns
