"""Error Handling tests."""
from unittest.mock import patch

import pytest
from pyspark.sql import types as T
from pyspark.errors.exceptions.captured import AnalysisException

from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import (
    flatten_dynamodb_struct
)
from dynamodb_curated_library.etl.transform.utils.exceptions import (
    FlattenStructureException
)


class TestErrorHandling:
    """Test error handling and suggestions."""

    def test_error_handling_raises_custom_exception(self, spark):
        """Test that error handling raises FlattenStructureException with suggestions."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"}
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)

        # Mock to force an error
        call_count = [0]
        original_select = df.select

        def mock_select(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return original_select(*args, **kwargs)
            else:
                raise AnalysisException("Simulated Spark error")

        with patch('pyspark.sql.DataFrame.select', side_effect=mock_select):
            with pytest.raises(FlattenStructureException) as exc_info:
                flatten_dynamodb_struct(df, max_depth=5, columns_as_json=None, parent_col="NewImage")

        # Verify exception contains useful information
        error_str = str(exc_info.value)
        assert "Failed to flatten DynamoDB structure" in error_str
        assert "current: 5" in error_str
        assert "SUGGESTIONS" in error_str

    def test_error_message_includes_parent_col(self, spark):
        """Test that error message includes parent_col value."""
        data = [
            {
                "Item": {
                    "orderId": {"S": "123"}
                }
            }
        ]

        schema = T.StructType([
            T.StructField("Item", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)

        # Mock to force an error
        call_count = [0]
        original_select = df.select

        def mock_select(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return original_select(*args, **kwargs)
            else:
                raise AnalysisException("Mock error")

        with patch('pyspark.sql.DataFrame.select', side_effect=mock_select):
            with pytest.raises(FlattenStructureException) as exc_info:
                flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="Item")

        error_str = str(exc_info.value)
        assert "parent_col='Item'" in error_str

    def test_exception_includes_suggestions(self, spark):
        """Test that exception includes actionable suggestions."""
        data = [
            {
                "NewImage": {
                    "orderId": {"S": "123"}
                }
            }
        ]

        schema = T.StructType([
            T.StructField("NewImage", T.StructType([
                T.StructField("orderId", T.StructType([T.StructField("S", T.StringType())]))
            ]))
        ])

        df = spark.createDataFrame(data, schema)

        # Mock to force an error
        call_count = [0]
        original_select = df.select

        def mock_select(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return original_select(*args, **kwargs)
            else:
                raise AnalysisException("Mock error")

        with patch('pyspark.sql.DataFrame.select', side_effect=mock_select):
            with pytest.raises(FlattenStructureException) as exc_info:
                flatten_dynamodb_struct(df, max_depth=3, columns_as_json=None, parent_col="NewImage")

        error_str = str(exc_info.value)
        assert "Reduce max_depth parameter" in error_str
        assert "columns_as_json" in error_str
        assert "Example:" in error_str
