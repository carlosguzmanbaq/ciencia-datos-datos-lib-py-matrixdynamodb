"""Basic Flattening tests."""

from pyspark.sql import types as T

from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import (
    flatten_dynamodb_struct
)


class TestBasicFlattening:
    """Test basic flattening scenarios."""

    def test_simple_structure(self, spark):
        """Test flattening simple DynamoDB structure."""
        # Create test data
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "amount": {"N": "100"},
                    "status": {"S": "completed"}
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("amount", T.StructType([T.StructField("N", T.StringType())])),
                T.StructField("status", T.StructType([T.StructField("S", T.StringType())]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        # Assertions
        assert result.count() == 1
        assert "order_id" in result.columns
        assert "amount" in result.columns
        assert "status" in result.columns

        row = result.first()
        assert row.order_id == "123"
        assert row.amount == "100"
        assert row.status == "completed"

    def test_nested_structure_level_2(self, spark):
        """Test flattening nested structure (2 levels)."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "state": {
                        "M": {
                            "status": {"S": "active"},
                            "code": {"N": "200"}
                        }
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("state", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("status", T.StructType([T.StructField("S", T.StringType())])),
                        T.StructField("code", T.StructType([T.StructField("N", T.StringType())]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        # Assertions
        assert "order_id" in result.columns
        assert "state_status" in result.columns
        assert "state_code" in result.columns

        row = result.first()
        assert row.order_id == "123"
        assert row.state_status == "active"
        assert row.state_code == "200"




class TestItemParentCol:
    """Test with 'Item' as parent_col (FULL export)."""

    def test_item_parent_col_simple(self, spark):
        """Test flattening with Item as parent column (simple structure)."""
        data = [
            {
                "Item": {
                    "orderId": {"S": "123"},
                    "amount": {"N": "100"}
                }
            }
        ]

        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("amount", T.StructType([T.StructField("N", T.StringType())]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="Item")

        # Assertions
        assert result.count() == 1
        assert "order_id" in result.columns
        assert "amount" in result.columns

        row = result.first()
        assert row.order_id == "123"
        assert row.amount == "100"

    def test_item_parent_col_nested(self, spark):
        """Test flattening with Item and nested structures."""
        data = [
            {
                "Item": {
                    "orderId": {"S": "123"},
                    "customer": {
                        "M": {
                            "name": {"S": "John Doe"},
                            "email": {"S": "john@example.com"}
                        }
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("customer", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("name", T.StructType([T.StructField("S", T.StringType())])),
                        T.StructField("email", T.StructType([T.StructField("S", T.StringType())]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="Item")

        # Assertions
        assert "order_id" in result.columns
        assert "customer_name" in result.columns
        assert "customer_email" in result.columns

        row = result.first()
        assert row.order_id == "123"
        assert row.customer_name == "John Doe"
        assert row.customer_email == "john@example.com"

    def test_item_with_depth_control(self, spark):
        """Test Item with max_depth control."""
        data = [
            {
                "Item": {
                    "orderId": {"S": "123"},
                    "metadata": {
                        "M": {
                            "level1": {
                                "M": {
                                    "level2": {
                                        "M": {
                                            "value": {"S": "deep"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("metadata", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("level1", T.StructType([
                            T.StructField("M", T.StructType([
                                T.StructField("level2", T.StructType([
                                    T.StructField("M", T.StructType([
                                        T.StructField("value", T.StructType([T.StructField("S", T.StringType())]))
                                    ]))
                                ]))
                            ]))
                        ]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=2, columns_as_json=None, parent_col="Item")

        # Assertions
        assert "order_id" in result.columns
        assert "metadata_level1" in result.columns

        row = result.first()
        assert row.order_id == "123"
        # metadata_level1 should be JSON (depth exceeded)
        assert isinstance(row.metadata_level1, str)

    def test_item_with_columns_as_json(self, spark):
        """Test Item with columns_as_json parameter."""
        data = [
            {
                "Item": {
                    "orderId": {"S": "123"},
                    "config": {
                        "M": {
                            "setting1": {"S": "value1"},
                            "setting2": {"S": "value2"}
                        }
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("config", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("setting1", T.StructType([T.StructField("S", T.StringType())])),
                        T.StructField("setting2", T.StructType([T.StructField("S", T.StringType())]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(
            df,
            max_depth=3,
            columns_as_json=["config"],
            parent_col="Item"
        )

        # Assertions
        assert "order_id" in result.columns
        assert "config" in result.columns

        row = result.first()
        assert row.order_id == "123"
        # config should be JSON string
        assert isinstance(row.config, str)
        assert "setting1" in row.config
