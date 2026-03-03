"""Columns As Json tests."""

from pyspark.sql import types as T

from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import (
    flatten_dynamodb_struct
)


class TestColumnsAsJson:
    """Test columns_as_json parameter."""

    def test_specific_column_as_json(self, spark):
        """Test converting specific column to JSON."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "state": {
                        "M": {
                            "status": {"S": "active"}
                        }
                    },
                    "metadata": {
                        "M": {
                            "key": {"S": "value"}
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
                        T.StructField("status", T.StructType([T.StructField("S", T.StringType())]))
                    ]))
                ])),
                T.StructField("metadata", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("key", T.StructType([T.StructField("S", T.StringType())]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(
            df,
            max_depth=3,
            columns_as_json=["metadata"],
            parent_col="NewImage"
        )

        # Assertions
        assert "order_id" in result.columns
        assert "state_status" in result.columns  # Should be flattened
        assert "metadata" in result.columns  # Should be JSON

        row = result.first()
        assert row.order_id == "123"
        assert row.state_status == "active"
        assert isinstance(row.metadata, str)
        assert "key" in row.metadata

    def test_multiple_columns_as_json(self, spark):
        """Test converting multiple columns to JSON."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "state": {
                        "M": {
                            "status": {"S": "active"}
                        }
                    },
                    "metadata": {
                        "M": {
                            "key": {"S": "value"}
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
                        T.StructField("status", T.StructType([T.StructField("S", T.StringType())]))
                    ]))
                ])),
                T.StructField("metadata", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("key", T.StructType([T.StructField("S", T.StringType())]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(
            df,
            max_depth=3,
            columns_as_json=["state", "metadata"],
            parent_col="NewImage"
        )

        # Assertions
        assert "order_id" in result.columns
        assert "state" in result.columns
        assert "metadata" in result.columns

        row = result.first()
        assert row.order_id == "123"
        assert isinstance(row.state, str)
        assert isinstance(row.metadata, str)
