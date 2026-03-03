from unittest.mock import Mock, patch

import json
import pytest

from dynamodb_curated_library.dev_utils.local_env import (
    LocalPathConverter,
    DevGlueReader,
    DevGlueContext,
    setup_colombian_locale
)


class TestLocalPathConverter:
    """Tests for LocalPathConverter class."""

    def test_s3_to_local_converts_s3_path(self):
        """Test S3 path conversion to local path."""
        s3_path = "s3://my-bucket/data/file.json"
        expected = "dev/catalog/my-bucket/data/file.json"
        assert LocalPathConverter.s3_to_local(s3_path) == expected

    def test_s3_to_local_returns_non_s3_path_unchanged(self):
        """Test non-S3 paths are returned unchanged."""
        local_path = "/local/path/file.json"
        assert LocalPathConverter.s3_to_local(local_path) == local_path

    def test_convert_paths_converts_multiple_paths(self):
        """Test conversion of multiple S3 paths."""
        paths = [
            "s3://bucket1/file1.json",
            "s3://bucket2/file2.json",
            "/local/file3.json"
        ]
        expected = [
            "dev/catalog/bucket1/file1.json",
            "dev/catalog/bucket2/file2.json",
            "/local/file3.json"
        ]
        assert LocalPathConverter.convert_paths(paths) == expected


class TestDevGlueReader:
    """Tests for DevGlueReader class."""

    def test_from_options_requires_connection_options(self, spark):
        """Test from_options raises error without connection_options."""
        reader = DevGlueReader(spark)
        with pytest.raises(ValueError, match="connection_options with 'paths' key is required"):
            reader.from_options()

    def test_from_options_requires_paths_key(self, spark):
        """Test from_options raises error without paths key."""
        reader = DevGlueReader(spark)
        with pytest.raises(ValueError, match="connection_options with 'paths' key is required"):
            reader.from_options(connection_options={})

    def test_from_options_reads_single_path(self, spark, tmp_path):
        """Test from_options reads data from single path."""
        # Create test JSON file
        test_file = tmp_path / "test.json"
        test_data = [{"id": "1", "name": "test"}]
        test_file.write_text(json.dumps(test_data[0]))

        reader = DevGlueReader(spark)
        df = reader.from_options(
            connection_options={"paths": [str(test_file)]}
        )

        assert df.count() == 1
        assert "id" in df.columns
        assert hasattr(df, 'toDF')

    def test_from_options_converts_string_path_to_list(self, spark, tmp_path):
        """Test from_options handles string path by converting to list."""
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps({"id": "1"}))

        reader = DevGlueReader(spark)
        df = reader.from_options(
            connection_options={"paths": str(test_file)}
        )

        assert df.count() == 1

    def test_from_catalog_requires_database_and_table(self, spark):
        """Test from_catalog raises error without required parameters."""
        reader = DevGlueReader(spark)

        with pytest.raises(ValueError, match="Both database and table_name are required"):
            reader.from_catalog(database="", table_name="table")

        with pytest.raises(ValueError, match="Both database and table_name are required"):
            reader.from_catalog(database="db", table_name="")

    def test_from_catalog_reads_from_hive(self, spark):
        """Test from_catalog reads from Hive metastore."""
        # Create test database and table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        test_data = [{"id": "1", "value": "test"}]
        df = spark.createDataFrame(test_data)
        df.write.saveAsTable("test_db.test_table", mode="overwrite")

        reader = DevGlueReader(spark)
        result_df = reader.from_catalog(database="test_db", table_name="test_table")

        assert result_df.count() == 1
        assert "id" in result_df.columns
        assert hasattr(result_df, 'toDF')

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_db.test_table")
        spark.sql("DROP DATABASE IF EXISTS test_db")


class TestDevGlueContext:
    """Tests for DevGlueContext class."""

    def test_initialization(self, spark):
        """Test DevGlueContext initializes correctly."""
        with patch('dynamodb_curated_library.dev_utils.local_env.setup_colombian_locale'):
            context = DevGlueContext(spark)

            assert context.spark_session == spark
            assert hasattr(context, 'create_data_frame')
            assert hasattr(context, 'create_dynamic_frame')
            assert isinstance(context.create_data_frame, DevGlueReader)

    def test_create_data_frame_is_dev_glue_reader(self, spark):
        """Test create_data_frame is instance of DevGlueReader."""
        with patch('dynamodb_curated_library.dev_utils.local_env.setup_colombian_locale'):
            context = DevGlueContext(spark)
            assert isinstance(context.create_data_frame, DevGlueReader)

    def test_create_dynamic_frame_is_dev_glue_reader(self, spark):
        """Test create_dynamic_frame is instance of DevGlueReader."""
        with patch('dynamodb_curated_library.dev_utils.local_env.setup_colombian_locale'):
            context = DevGlueContext(spark)
            assert isinstance(context.create_dynamic_frame, DevGlueReader)


class TestSetupColombianLocale:
    """Tests for setup_colombian_locale function."""

    def test_setup_colombian_locale_success(self, capsys):
        """Test successful locale setup."""
        mock_jvm = Mock()
        mock_locale_class = Mock()
        mock_colombian_locale = Mock()

        mock_jvm.java.util.Locale = mock_locale_class
        mock_locale_class.return_value = mock_colombian_locale
        mock_locale_class.getDefault.return_value = "es_CO"

        setup_colombian_locale(mock_jvm)

        mock_locale_class.assert_called_with("es", "CO")
        mock_locale_class.setDefault.assert_called_with(mock_colombian_locale)

        captured = capsys.readouterr()
        assert "Default Locale set to:" in captured.out

    def test_setup_colombian_locale_handles_exception(self, capsys):
        """Test locale setup handles exceptions gracefully."""
        mock_jvm = Mock()
        mock_jvm.java.util.Locale.side_effect = Exception("JVM error")

        setup_colombian_locale(mock_jvm)

        captured = capsys.readouterr()
        assert "Warning: Could not set Colombian locale:" in captured.out
