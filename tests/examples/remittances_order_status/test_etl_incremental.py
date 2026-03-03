
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import types as T
from pyspark.sql import Row

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.core.config.job_parameters import JobParameters
from dynamodb_curated_library.etl.extract.catalog import Catalog
from dynamodb_curated_library.etl.extract.raw_sources import RawSources
from dynamodb_curated_library.etl.transform.transformations import FlattenTransformations
from dynamodb_curated_library.metadata.metadata import MetadataConfig

from examples.co_delfos_productos_srf_transformar_remittances_order_status.config.constans import building_constants
from examples.co_delfos_productos_srf_transformar_remittances_order_status.config.sources_dictionary import get_raw_dynamodb_source
from examples.co_delfos_productos_srf_transformar_remittances_order_status.transform.transformations import DynamoDBTableTransformations


@pytest.fixture(name="mock_dynamodb_incremental_data")
def mock_dynamodb_incremental_data_fixture(spark):
    """Create mock DynamoDB INCREMENTAL export data."""

    data = [
        Row(
            Keys=Row(
                order_id=Row(S="ORDER001"),
                remittance_id=Row(S="REM001")
            ),
            NewImage=Row(
                order_id=Row(S="ORDER001"),
                remittance_id=Row(S="REM001"),
                customer_nombre=Row(S="John Updated"),
                order_date=Row(S="2024-01-15T10:30:00Z"),
                state_date=Row(S="2024-01-15T15:00:00Z")
            ),
            eventName="MODIFY"
        ),
        Row(
            Keys=Row(
                order_id=Row(S="ORDER003"),
                remittance_id=Row(S="REM003")
            ),
            NewImage=Row(
                order_id=Row(S="ORDER003"),
                remittance_id=Row(S="REM003"),
                customer_nombre=Row(S="New Customer"),
                order_date=Row(S="2024-01-15T14:00:00Z"),
                state_date=Row(S="2024-01-15T14:30:00Z")
            ),
            eventName="INSERT"
        )
    ]
    return spark.createDataFrame(data)


@pytest.fixture(name="constants")
def constants_fixture():
    """Create test constants."""
    return building_constants()


@pytest.fixture(name="job_params")
def job_params_fixture():
    """Create test job parameters for INCREMENTAL process."""
    params = Mock(spec=JobParameters)
    params.process_date = "2024-01-15 14"
    params.account = "123456789"
    params.env = "dev"
    params.process_type = "INC"
    params.is_datalab = False
    params.log_level = "INFO"
    params.event_status = False
    return params


def test_incremental_etl_pipeline(glue_context, mock_dynamodb_incremental_data, constants, job_params):
    """Test complete ETL pipeline for INCREMENTAL process type."""

    # ========== Configuration ==========
    job_config = JobConfig(
        params=job_params,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params.process_type)
    )

    assert job_config.process_type == "INC"
    assert job_config.process_date == "2024-01-15 14"

    # ========== Extract ==========
    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=mock_dynamodb_incremental_data)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
    )
    sources = RawSources(catalog=catalog)

    assert sources.dynamodb_table is not None
    assert sources.dynamodb_table.count() == 2

    # ========== Transform ==========
    flatten_transformations = FlattenTransformations(
        job_config=job_config,
        sources=sources
    )
    df_flatten, df_keys, df_raw, record_count = flatten_transformations.get_flatten_table()

    assert record_count == 2
    assert df_flatten is not None
    assert df_keys is not None
    assert df_raw is not None
    assert "order_id" in df_flatten.columns
    assert "remittance_id" in df_flatten.columns

    # Business transformations
    dynamodb_table_transforms = DynamoDBTableTransformations(
       df_flatten=df_flatten,
       df_keys=df_keys,
       df_raw=df_raw,
       job_config=job_config,
       sources=sources
    )
    df_transform = dynamodb_table_transforms.get_main_table()

    assert df_transform is not None
    assert "id_reward" in df_transform.columns
    assert df_transform.count() == 2

    # Metadata
    with patch('dynamodb_curated_library.metadata.metadata.MetadataConfig.load_metadata') as mock_load:
        mock_load.return_value = {}

        metadata_config = MetadataConfig(
            file_name=job_config.table_name,
            constants=constants,
            target_module="metadata"
        )
        df_final = metadata_config.add_metadata_columns(df=df_transform, job_config=job_config)

        assert df_final is not None
        assert "momento_ingestion" in df_final.columns
        assert "job_process_date" in df_final.columns
        assert "job_process_type" in df_final.columns

    # ========== Load ==========
    # Verify final DataFrame is ready for save
    assert df_final.count() == 2
    assert len(df_final.columns) > 0


def test_incremental_etl_pipeline_empty_data(spark, glue_context, constants, job_params):
    """Test ETL pipeline handles empty data gracefully for INCREMENTAL process."""

    # Create empty DataFrame
    empty_df = spark.createDataFrame([], T.StructType([]))

    job_config = JobConfig(
        params=job_params,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params.process_type)
    )

    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=empty_df)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
    )
    sources = RawSources(catalog=catalog)

    flatten_transformations = FlattenTransformations(
        job_config=job_config,
        sources=sources
    )
    result = flatten_transformations.get_flatten_table()

    # Should return None for empty data
    assert result == (None, None, None, 0)


def test_incremental_with_delete_events(spark, glue_context, constants, job_params):
    """Test INCREMENTAL process handles DELETE events (no NewImage)."""

    # Define schema for DELETE events with nullable NewImage
    schema = T.StructType([
        T.StructField("Keys", T.StructType([
            T.StructField("order_id", T.StructType([T.StructField("S", T.StringType())])),
            T.StructField("remittance_id", T.StructType([T.StructField("S", T.StringType())]))
        ])),
        T.StructField("NewImage", T.StructType([
            T.StructField("order_id", T.StructType([T.StructField("S", T.StringType())])),
            T.StructField("remittance_id", T.StructType([T.StructField("S", T.StringType())]))
        ]), nullable=True),
        T.StructField("eventName", T.StringType())
    ])

    # DELETE events don't have NewImage
    data = [
        Row(
            Keys=Row(
                order_id=Row(S="ORDER999"),
                remittance_id=Row(S="REM999")
            ),
            NewImage=None,
            eventName="REMOVE"
        )
    ]
    df_with_deletes = spark.createDataFrame(data, schema=schema)

    job_config = JobConfig(
        params=job_params,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params.process_type)
    )

    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=df_with_deletes)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
    )
    sources = RawSources(catalog=catalog)

    flatten_transformations = FlattenTransformations(
        job_config=job_config,
        sources=sources
    )

    # DELETE events with null NewImage should still be processed
    df_flatten, df_keys, df_raw, record_count = flatten_transformations.get_flatten_table()
    assert record_count == 1
    assert df_flatten is not None
    assert df_keys is not None
    assert df_raw is not None
