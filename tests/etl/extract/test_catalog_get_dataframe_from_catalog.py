from unittest.mock import Mock, patch
from datetime import datetime

import pytest
from pyspark.sql import DataFrame

from dynamodb_curated_library.etl.extract.catalog import Catalog, CatalogException


class TestCatalogGetDataframeFromCatalog:
    """Test cases for Catalog.get_dataframe_from_catalog method"""

    @pytest.fixture
    def mock_glue_context(self):
        """Mock GlueContext for testing"""
        glue_context = Mock()
        glue_context.create_data_frame = Mock()
        return glue_context

    @pytest.fixture
    def mock_job_config(self):
        """Mock JobConfig for testing"""
        job_config = Mock()
        job_config.table_catalog_names.curated_database = "test_database"
        job_config.table_name = "test_table"
        job_config.process_day_parts_str = ("2024", "01", "15")
        job_config.process_type = "FULL"
        job_config.start_process_date = datetime(2024, 1, 15, 14, 0, 0)
        job_config.is_datalab = False
        job_config.buckets.raw_bucket = "test-bucket"
        job_config.dynamodb_export.export_folder_name = "export"
        job_config.constants.domain = "test_domain"
        job_config.constants.country_clan = "co"
        job_config.constants.subdomain = "test_subdomain"
        job_config.dynamodb_export.dynamodb_process_type_folder = "full"
        job_config.account = "123456"
        job_config.env = "pdn"
        job_config.table_source = {"s3_uri": "s3://test/path"}
        return job_config

    @pytest.fixture
    def mock_dataframe(self):
        """Mock DataFrame for testing"""
        df = Mock(spec=DataFrame)
        df.select = Mock(return_value=df)
        return df

    @pytest.fixture
    def catalog(self, mock_glue_context, mock_job_config):
        """Create Catalog instance for testing"""
        with patch('dynamodb_curated_library.etl.extract.catalog.ConfigTable'):
            return Catalog(mock_glue_context, mock_job_config)

    def test_get_dataframe_from_catalog_success(self, catalog, mock_job_config, mock_dataframe):
        """Test successful DataFrame reading from Glue Catalog"""
        # Arrange
        catalog.glue_context.create_data_frame.from_catalog.return_value = mock_dataframe

        # Act
        result = catalog.get_dataframe_from_catalog(mock_job_config)

        # Assert
        catalog.glue_context.create_data_frame.from_catalog.assert_called_once_with(
            database="test_database",
            table_name="test_table"
        )
        assert result == mock_dataframe

    def test_get_dataframe_from_catalog_with_select_columns(self, catalog, mock_job_config, mock_dataframe):
        """Test DataFrame reading with column selection"""
        # Arrange
        catalog.glue_context.create_data_frame.from_catalog.return_value = mock_dataframe
        select_columns = ["col1", "col2", "col3"]

        # Act
        result = catalog.get_dataframe_from_catalog(mock_job_config, select_columns)

        # Assert
        catalog.glue_context.create_data_frame.from_catalog.assert_called_once_with(
            database="test_database",
            table_name="test_table"
        )
        mock_dataframe.select.assert_called_once_with("col1", "col2", "col3")
        assert result == mock_dataframe

    def test_get_dataframe_from_catalog_without_select_columns(self, catalog, mock_job_config, mock_dataframe):
        """Test DataFrame reading without column selection"""
        # Arrange
        catalog.glue_context.create_data_frame.from_catalog.return_value = mock_dataframe

        # Act
        result = catalog.get_dataframe_from_catalog(mock_job_config, None)

        # Assert
        catalog.glue_context.create_data_frame.from_catalog.assert_called_once_with(
            database="test_database",
            table_name="test_table"
        )
        mock_dataframe.select.assert_not_called()
        assert result == mock_dataframe

    def test_get_dataframe_from_catalog_empty_select_columns(self, catalog, mock_job_config, mock_dataframe):
        """Test DataFrame reading with empty column list"""
        # Arrange
        catalog.glue_context.create_data_frame.from_catalog.return_value = mock_dataframe
        select_columns = []

        # Act
        result = catalog.get_dataframe_from_catalog(mock_job_config, select_columns)

        # Assert
        catalog.glue_context.create_data_frame.from_catalog.assert_called_once_with(
            database="test_database",
            table_name="test_table"
        )
        mock_dataframe.select.assert_not_called()
        assert result == mock_dataframe

    def test_get_dataframe_from_catalog_glue_exception(self, catalog, mock_job_config):
        """Test CatalogException when Glue Catalog read fails"""
        # Arrange
        catalog.glue_context.create_data_frame.from_catalog.side_effect = Exception("Glue error")

        # Act & Assert
        with pytest.raises(CatalogException) as exc_info:
            catalog.get_dataframe_from_catalog(mock_job_config)

        expected_error = (
            "Error reading from Glue Catalog - "
            "Database: test_database, "
            "Table: test_table"
        )
        assert str(exc_info.value) == expected_error
        assert exc_info.value.__cause__.args[0] == "Glue error"

    def test_get_dataframe_from_catalog_select_exception(self, catalog, mock_job_config, mock_dataframe):
        """Test CatalogException when DataFrame select fails"""
        # Arrange
        catalog.glue_context.create_data_frame.from_catalog.return_value = mock_dataframe
        mock_dataframe.select.side_effect = Exception("Select error")
        select_columns = ["invalid_col"]

        # Act & Assert
        with pytest.raises(CatalogException) as exc_info:
            catalog.get_dataframe_from_catalog(mock_job_config, select_columns)

        expected_error = (
            "Error reading from Glue Catalog - "
            "Database: test_database, "
            "Table: test_table"
        )
        assert str(exc_info.value) == expected_error
        assert exc_info.value.__cause__.args[0] == "Select error"

    def test_get_dataframe_from_catalog_different_job_config(self, catalog, mock_dataframe):
        """Test with different job configuration values"""
        # Arrange
        job_config = Mock()
        job_config.table_catalog_names.curated_database = "another_database"
        job_config.table_name = "another_table"
        catalog.glue_context.create_data_frame.from_catalog.return_value = mock_dataframe

        # Act
        result = catalog.get_dataframe_from_catalog(job_config)

        # Assert
        catalog.glue_context.create_data_frame.from_catalog.assert_called_once_with(
            database="another_database",
            table_name="another_table"
        )
        assert result == mock_dataframe
