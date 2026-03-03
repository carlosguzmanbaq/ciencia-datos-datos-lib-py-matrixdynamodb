"""Depth Control tests."""

from pyspark.sql import types as T

from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import (
    flatten_dynamodb_struct
)


class TestMaxDepthControl:
    """Test max_depth parameter."""

    def test_max_depth_2(self, spark):
        """Test max_depth=2 converts deeper structures to JSON."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "level1": {
                        "M": {
                            "level2": {
                                "M": {
                                    "level3": {
                                        "M": {
                                            "deepValue": {"S": "deep"}
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
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("level1", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("level2", T.StructType([
                            T.StructField("M", T.StructType([
                                T.StructField("level3", T.StructType([
                                    T.StructField("M", T.StructType([
                                        T.StructField("deepValue", T.StructType([
                                            T.StructField("S", T.StringType())
                                        ]))
                                    ]))
                                ]))
                            ]))
                        ]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=2, columns_as_json=None, parent_col="NewImage")

        # Assertions
        assert "order_id" in result.columns
        assert "level1_level2" in result.columns  # Should be JSON (depth 2, contains level 3)

        row = result.first()
        assert row.order_id == "123"
        # level1_level2 should be JSON string containing level3
        assert isinstance(row.level1_level2, str)
        assert "level3" in row.level1_level2

    def test_max_depth_1(self, spark):
        """Test max_depth=1 converts level 2+ to JSON."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "metadata": {
                        "M": {
                            "key1": {"S": "value1"},
                            "key2": {"S": "value2"}
                        }
                    }
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("metadata", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("key1", T.StructType([T.StructField("S", T.StringType())])),
                        T.StructField("key2", T.StructType([T.StructField("S", T.StringType())]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=1, columns_as_json=None, parent_col="NewImage")

        # Assertions
        assert "order_id" in result.columns
        assert "metadata" in result.columns

        row = result.first()
        assert row.order_id == "123"
        # metadata should be JSON string
        assert isinstance(row.metadata, str)
        assert "key1" in row.metadata




class TestCombinedScenarios:
    """Test combined scenarios."""

    def test_max_depth_and_columns_as_json(self, spark):
        """Test combining max_depth and columns_as_json."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "level1": {
                        "M": {
                            "level2": {
                                "M": {
                                    "value": {"S": "test"}
                                }
                            }
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
                T.StructField("level1", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("level2", T.StructType([
                            T.StructField("M", T.StructType([
                                T.StructField("value", T.StructType([T.StructField("S", T.StringType())]))
                            ]))
                        ]))
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
            parent_col="NewImage",
            max_depth=3,
            columns_as_json=["metadata"]
        )

        # Assertions
        assert "order_id" in result.columns
        assert "level1_level2_value" in result.columns  # Flattened (within max_depth)
        assert "metadata" in result.columns  # JSON (user specified)

        row = result.first()
        assert row.order_id == "123"
        assert row.level1_level2_value == "test"
        assert isinstance(row.metadata, str)
