"""Edge Cases tests."""
import logging

from pyspark.sql import types as T
from pyspark.sql import functions as F

from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import (
    flatten_dynamodb_struct
)


class TestComplexErrorScenarios:
    """Test complex error scenarios that might occur in production."""

    def test_varying_schema_between_records(self, spark):
        """Test handling of varying schemas between records (common DynamoDB issue)."""
        # This simulates records with different structures
        # Record 1 has simple structure, Record 2 has deep nesting
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "simple": {"S": "value"}
                }
            },
            {
                "NewImage": {
                    "orderId": {"S": "456"},
                    "complex": {
                        "M": {
                            "level1": {
                                "M": {
                                    "level2": {
                                        "M": {
                                            "level3": {
                                                "M": {
                                                    "level4": {"S": "deep"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]

        # Create schema that accommodates both structures
        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("simple", T.StructType([T.StructField("S", T.StringType())]), nullable=True),
                T.StructField("complex", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("level1", T.StructType([
                            T.StructField("M", T.StructType([
                                T.StructField("level2", T.StructType([
                                    T.StructField("M", T.StructType([
                                        T.StructField("level3", T.StructType([
                                            T.StructField("M", T.StructType([
                                                T.StructField("level4", T.StructType([
                                                    T.StructField("S", T.StringType())
                                                ]))
                                            ]))
                                        ]))
                                    ]))
                                ]))
                            ]))
                        ]))
                    ]))
                ]), nullable=True)
            ]))
        ])

        df = spark.createDataFrame(data, schema)

        # With max_depth=2, should convert deep structure to JSON
        result = flatten_dynamodb_struct(df, max_depth=2, columns_as_json=None, parent_col="NewImage")

        assert result.count() == 2
        assert "order_id" in result.columns

        # First record should have simple field
        first_row = result.filter(F.col("order_id") == "123").first()
        assert first_row.simple == "value"

        # Second record should have complex_level1 as JSON
        second_row = result.filter(F.col("order_id") == "456").first()
        assert "complex_level1" in result.columns
        # Verify the complex field was converted to JSON at level 2
        assert second_row.complex_level1 is not None

    def test_null_values_in_nested_structures(self, spark):
        """Test handling of null values in nested structures."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "metadata": {
                        "M": {
                            "key1": {"S": "value1"},
                            "key2": {"NULL": True}
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
                        T.StructField("key2", T.StructType([T.StructField("NULL", T.BooleanType())]))
                    ]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        assert result.count() == 1
        row = result.first()
        assert row.order_id == "123"
        assert row.metadata_key1 == "value1"

    def test_empty_structures(self, spark):
        """Test handling of empty nested structures."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "emptyMap": {"M": {}}
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("emptyMap", T.StructType([
                    T.StructField("M", T.StructType([]))
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        assert result.count() == 1
        assert "order_id" in result.columns

    def test_mixed_types_in_same_field(self, spark):
        """Test handling when same field has different types across records."""
        # This tests the scenario where DynamoDB allows different types
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "flexField": {"S": "string_value"}
                }
            },
            {
                "NewImage": {
                    "orderId": {"S": "456"},
                    "flexField": {"N": "999"}
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())])),
                T.StructField("flexField", T.StructType([
                    T.StructField("S", T.StringType(), nullable=True),
                    T.StructField("N", T.StringType(), nullable=True)
                ]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)
        result = flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        # Should use coalesce to handle both types
        assert result.count() == 2
        rows = result.collect()
        assert rows[0].flex_field == "string_value"
        assert rows[1].flex_field == "999"

    def test_deeply_nested_with_columns_as_json_recovery(self, spark, caplog):
        """Test that deeply nested structure can be recovered using columns_as_json."""

        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"},
                    "veryDeep": {
                        "M": {
                            "l1": {
                                "M": {
                                    "l2": {
                                        "M": {
                                            "l3": {
                                                "M": {
                                                    "l4": {
                                                        "M": {
                                                            "l5": {"S": "value"}
                                                        }
                                                    }
                                                }
                                            }
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
                T.StructField("veryDeep", T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("l1", T.StructType([
                            T.StructField("M", T.StructType([
                                T.StructField("l2", T.StructType([
                                    T.StructField("M", T.StructType([
                                        T.StructField("l3", T.StructType([
                                            T.StructField("M", T.StructType([
                                                T.StructField("l4", T.StructType([
                                                    T.StructField("M", T.StructType([
                                                        T.StructField("l5", T.StructType([
                                                            T.StructField("S", T.StringType())
                                                        ]))
                                                    ]))
                                                ]))
                                            ]))
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

        # First attempt with low max_depth - should convert to JSON automatically
        with caplog.at_level(logging.DEBUG):
            result = flatten_dynamodb_struct(df, max_depth=2, columns_as_json=None, parent_col="NewImage")
            assert result.count() == 1

            # Verify deep structure was converted to JSON
            assert any("Converting to JSON" in record.message for record in caplog.records)

        # Second attempt with explicit columns_as_json
        result2 = flatten_dynamodb_struct(
            df,
            max_depth=3,
            columns_as_json=["veryDeep"],
            parent_col="NewImage"
        )
        assert result2.count() == 1
        row = result2.first()
        assert isinstance(row.very_deep, str)
        assert "l1" in row.very_deep
