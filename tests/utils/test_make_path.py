from unittest.mock import Mock, patch

from dynamodb_curated_library.utils.make_path import (
    get_metadata,
    get_schemas_path
)


class TestGetMetadata:
    """Tests for get_metadata function."""

    @patch('dynamodb_curated_library.utils.make_path.get_schemas_path')
    def test_get_metadata_with_yml_extension(self, mock_get_schemas_path):
        """Test get_metadata with file name already having .yml extension."""
        mock_get_schemas_path.return_value = "/path/to/schemas"
        mock_module = Mock()

        result = get_metadata("test_file.yml", mock_module, "metadata")

        assert result == "/path/to/schemas/test_file.yml"
        mock_get_schemas_path.assert_called_once_with(module=mock_module, target_module="metadata")

    @patch('dynamodb_curated_library.utils.make_path.get_schemas_path')
    def test_get_metadata_without_yml_extension(self, mock_get_schemas_path):
        """Test get_metadata adds .yml extension when missing."""
        mock_get_schemas_path.return_value = "/path/to/schemas"
        mock_module = Mock()

        result = get_metadata("test_file", mock_module, "metadata")

        assert result == "/path/to/schemas/test_file.yml"
        mock_get_schemas_path.assert_called_once_with(module=mock_module, target_module="metadata")

    @patch('dynamodb_curated_library.utils.make_path.get_schemas_path')
    def test_get_metadata_with_different_target_module(self, mock_get_schemas_path):
        """Test get_metadata with different target module."""
        mock_get_schemas_path.return_value = "/path/to/config"
        mock_module = Mock()

        result = get_metadata("config_file", mock_module, "config")

        assert result == "/path/to/config/config_file.yml"
        mock_get_schemas_path.assert_called_once_with(module=mock_module, target_module="config")


class TestGetSchemasPath:
    """Tests for get_schemas_path function."""

    @patch('dynamodb_curated_library.utils.make_path.impresources.files')
    def test_get_schemas_path_regular_module(self, mock_files):
        """Test get_schemas_path with regular (non-whl) module."""
        mock_files.return_value = "/regular/path/to/module"
        mock_module = Mock()

        result = get_schemas_path(mock_module, "schemas")

        assert result == "/regular/path/to/module"
        mock_files.assert_called_once_with(mock_module)

    @patch('dynamodb_curated_library.utils.make_path.get_unziped_path')
    @patch('dynamodb_curated_library.utils.make_path.impresources.files')
    def test_get_schemas_path_whl_module(self, mock_files, mock_get_unziped_path):
        """Test get_schemas_path with .whl packaged module."""
        mock_files.return_value = "/path/to/module.whl/subdir"
        mock_get_unziped_path.return_value = "/unzipped/path/schemas"
        mock_module = Mock()

        result = get_schemas_path(mock_module, "schemas")

        assert result == "/unzipped/path/schemas"
        mock_files.assert_called_once_with(mock_module)
        mock_get_unziped_path.assert_called_once_with("/path/to/module.whl/subdir", "schemas")

    @patch('dynamodb_curated_library.utils.make_path.impresources.files')
    def test_get_schemas_path_with_whl_in_path(self, mock_files):
        """Test get_schemas_path detects .whl in path."""
        mock_files.return_value = "/some/path/package.whl/internal/path"
        mock_module = Mock()

        with patch('dynamodb_curated_library.utils.make_path.get_unziped_path') as mock_unzip:
            mock_unzip.return_value = "/extracted/path"
            result = get_schemas_path(mock_module, "target")

            assert result == "/extracted/path"
            mock_unzip.assert_called_once()
