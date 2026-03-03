from unittest.mock import Mock, patch

import pytest

from dynamodb_curated_library.etl.load.save import (
    SaveException,
    save,
    save_hudi,
    _save_local
)


class TestSave:
    """Tests for save function."""

    def test_save_raises_error_if_df_is_none(self):
        """Test save raises ValueError if df is None."""
        with pytest.raises(ValueError, match="df cannot be None"):
            save(df=None, hudi_options={}, path="s3://bucket/path")

    def test_save_raises_error_if_hudi_options_is_none(self, spark):
        """Test save raises ValueError if hudi_options is None."""
        df = spark.createDataFrame([{"id": "1"}])
        with pytest.raises(ValueError, match="hudi_options cannot be None"):
            save(df=df, hudi_options=None, path="s3://bucket/path")

    def test_save_raises_error_if_path_is_none(self, spark):
        """Test save raises ValueError if path is None."""
        df = spark.createDataFrame([{"id": "1"}])
        with pytest.raises(ValueError, match="path cannot be None"):
            save(df=df, hudi_options={}, path=None)

    @patch('dynamodb_curated_library.etl.load.save.IS_LOCAL_ENV', False)
    def test_save_calls_write_in_non_local_env(self, spark, capsys):
        """Test save writes to S3 in non-local environment."""
        df = spark.createDataFrame([{"id": "1", "value": "test"}])
        hudi_options = {
            "hoodie.table.name": "test_table",
            "hoodie.datasource.hive_sync.database": "test_db"
        }
        path = "s3://test-bucket/data"

        # Mock the DataFrameWriter chain
        mock_save = Mock()
        mock_options = Mock(return_value=Mock(save=mock_save))
        mock_mode = Mock(return_value=Mock(options=mock_options))
        mock_format = Mock(return_value=Mock(mode=mock_mode))

        with patch.object(type(df.write), 'format', mock_format):
            save(df=df, hudi_options=hudi_options, path=path, write_format="parquet", mode="overwrite")

            captured = capsys.readouterr()
            assert "Saving catalog: test_db.test_table" in captured.out
            mock_format.assert_called_once_with("parquet")
            mock_mode.assert_called_once_with("overwrite")
            mock_save.assert_called_once_with(path)

    @patch('dynamodb_curated_library.etl.load.save.IS_LOCAL_ENV', True)
    @patch('dynamodb_curated_library.etl.load.save._save_local')
    def test_save_calls_save_local_in_local_env(self, mock_save_local, spark):
        """Test save calls _save_local in local environment."""
        df = spark.createDataFrame([{"id": "1"}])
        hudi_options = {"hoodie.table.name": "test_table"}
        path = "s3://test-bucket/data"

        save(df=df, hudi_options=hudi_options, path=path)

        mock_save_local.assert_called_once_with(df, hudi_options)

    @patch('dynamodb_curated_library.etl.load.save.IS_LOCAL_ENV', False)
    def test_save_raises_save_exception_on_error(self, spark):
        """Test save raises SaveException when write fails."""
        df = spark.createDataFrame([{"id": "1"}])
        hudi_options = {
            "hoodie.table.name": "test_table",
            "hoodie.datasource.hive_sync.database": "test_db"
        }
        path = "s3://test-bucket/data"

        # Mock the format method to raise exception
        mock_format = Mock(side_effect=Exception("Write failed"))

        with patch.object(type(df.write), 'format', mock_format):
            with pytest.raises(SaveException, match="Failed to save DataFrame to s3://test-bucket/data"):
                save(df=df, hudi_options=hudi_options, path=path)


class TestSaveLocal:
    """Tests for _save_local function."""

    def test_save_local_raises_error_if_table_missing(self, spark):
        """Test _save_local raises ValueError if table name is missing."""
        df = spark.createDataFrame([{"id": "1"}])
        hudi_options = {"hoodie.datasource.hive_sync.database": "test_db"}

        with pytest.raises(ValueError, match="hudi_options must contain"):
            _save_local(df, hudi_options)

    def test_save_local_raises_error_if_database_missing(self, spark):
        """Test _save_local raises ValueError if database is missing."""
        df = spark.createDataFrame([{"id": "1"}])
        hudi_options = {"hoodie.table.name": "test_table"}

        with pytest.raises(ValueError, match="hudi_options must contain"):
            _save_local(df, hudi_options)

    def test_save_local_creates_database_and_saves_table(self, spark, capsys, tmp_path, monkeypatch):
        """Test _save_local creates database and saves table."""
        # Change working directory to tmp_path
        monkeypatch.chdir(tmp_path)

        df = spark.createDataFrame([{"id": "1", "value": "test"}])
        hudi_options = {
            "hoodie.table.name": "test_table",
            "hoodie.datasource.hive_sync.database": "test_db"
        }

        _save_local(df, hudi_options)

        # Verify database was created
        databases = [row.namespace for row in spark.sql("SHOW DATABASES").collect()]
        assert "test_db" in databases

        # Verify table was created
        tables = [row.tableName for row in spark.sql("SHOW TABLES IN test_db").collect()]
        assert "test_table" in tables

        # Verify output message
        captured = capsys.readouterr()
        assert "Saving to local catalog: test_db.test_table" in captured.out

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test_db.test_table")
        spark.sql("DROP DATABASE IF EXISTS test_db")


class TestSaveHudi:
    """Tests for save_hudi function."""

    @patch('dynamodb_curated_library.etl.load.save.save')
    def test_save_hudi_calls_save_with_hudi_format(self, mock_save, spark):
        """Test save_hudi calls save with hudi format."""
        df = spark.createDataFrame([{"id": "1"}])
        hudi_options = {"hoodie.table.name": "test_table"}
        path = "s3://test-bucket/data"
        mode = "append"

        save_hudi(df=df, hudi_options=hudi_options, path=path, mode=mode)

        mock_save.assert_called_once_with(
            df=df,
            hudi_options=hudi_options,
            path=path,
            write_format="hudi",
            mode=mode
        )

    @patch('dynamodb_curated_library.etl.load.save.save')
    def test_save_hudi_uses_default_mode(self, mock_save, spark):
        """Test save_hudi uses default mode 'overwrite'."""
        df = spark.createDataFrame([{"id": "1"}])
        hudi_options = {"hoodie.table.name": "test_table"}
        path = "s3://test-bucket/data"

        save_hudi(df=df, hudi_options=hudi_options, path=path)

        mock_save.assert_called_once_with(
            df=df,
            hudi_options=hudi_options,
            path=path,
            write_format="hudi",
            mode="overwrite"
        )
