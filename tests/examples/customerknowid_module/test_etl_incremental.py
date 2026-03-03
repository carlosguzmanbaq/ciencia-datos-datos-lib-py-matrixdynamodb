from unittest.mock import Mock, patch

import json
import pytest
from pyspark.sql import types as T, Row

from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.extract.catalog import Catalog
from dynamodb_curated_library.etl.extract.raw_sources import RawSources
from dynamodb_curated_library.etl.transform.transformations import FlattenTransformations


from examples.co_delfos_clientes_prn_transformar_customerknowid_module.config.sources_dictionary import get_raw_dynamodb_source
from examples.co_delfos_clientes_prn_transformar_customerknowid_module.transform.transformations import DynamoDBTableTransformations


@pytest.fixture(name="mock_dynamodb_incremental_data")
def mock_dynamodb_incremental_data_fixture(spark):
    """Create mock DynamoDB INCREMENTAL export data with INSERT and UPDATE."""
    data = [
        {
            "Keys": {"customerId": {"S": "CUST001"}},
            "NewImage": {
                "customerId": {"S": "CUST001"},
                "email": {"S": "john.updated@example.com"},
                "firstName": {"S": "John Updated"},
                "phoneNumber": {"S": "+573001234567"},
                "creationDate": {"S": "2024-01-15T10:00:00Z"},
                "updateDate": {"S": "2024-01-15T15:00:00Z"},
                "owner": {"S": "system"},
                "risk": {"S": "LOW"},
                "products": {
                    "M": {
                        "credits": {"BOOL": True},
                        "remittances": {"BOOL": True}
                    }
                }
            },
            "eventName": "MODIFY"
        },
        {
            "Keys": {"customerId": {"S": "CUST003"}},
            "NewImage": {
                "customerId": {"S": "CUST003"},
                "email": {"S": "new@example.com"},
                "firstName": {"S": "New Customer"},
                "phoneNumber": {"S": "+573009999999"},
                "creationDate": {"S": "2024-01-15T14:00:00Z"},
                "updateDate": {"S": "2024-01-15T14:00:00Z"},
                "owner": {"S": "system"},
                "risk": {"S": "HIGH"},
                "products": {
                    "M": {
                        "credits": {"BOOL": False},
                        "remittances": {"BOOL": True}
                    }
                }
            },
            "eventName": "INSERT"
        }
    ]
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(d) for d in data]))


@pytest.fixture(name="mock_existing_curated_table")
def mock_existing_curated_table_fixture(spark):
    """Create mock existing curated table for event_status comparison."""
    data = [
        Row(customer_id="CUST001"),
        Row(customer_id="CUST002")
    ]
    return spark.createDataFrame(data)


def test_incremental_etl_with_event_status(
        glue_context,
        mock_dynamodb_incremental_data,
        mock_existing_curated_table,
        constants,
        job_params_inc
    ):
    """Test INCREMENTAL ETL with event_status determining INSERT vs UPDATE."""

    # ========== Configuration ==========
    job_config = JobConfig(
        params=job_params_inc,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params_inc.process_type)
    )

    assert job_config.process_type == "INC"
    assert job_config.event_status is True

    # ========== Extract ==========
    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=mock_dynamodb_incremental_data)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
    )
    sources = RawSources(catalog=catalog)

    assert sources.dynamodb_table.count() == 2

    # Mock catalog.get_dataframe_from_catalog for event_status comparison
    with patch.object(catalog, 'get_dataframe_from_catalog', return_value=mock_existing_curated_table):
        # ========== Transform ==========
        flatten_transformations = FlattenTransformations(
            job_config=job_config,
            sources=sources
        )
        df_flatten, df_keys, df_raw, record_count = flatten_transformations.get_flatten_table()

        assert record_count == 2
        assert "flag_event_status" in df_flatten.columns

        # Verify event_status flags
        event_statuses = {
            row.customer_id: row.flag_event_status
            for row in df_flatten.select("customer_id", "flag_event_status").collect()
        }

        assert event_statuses["CUST001"] == "UPDATED"  # Exists in curated table
        assert event_statuses["CUST003"] == "INSERTED"  # New customer

        # ========== Business Transformations ==========
        dynamodb_table_transforms = DynamoDBTableTransformations(
            df_flatten=df_flatten,
            df_keys=df_keys,
            df_raw=df_raw,
            job_config=job_config,
            sources=sources
        )
        df_transform = dynamodb_table_transforms.get_main_table()

        assert df_transform is not None
        assert "flag_event_status" in df_transform.columns


def test_incremental_with_delete_events(spark, glue_context, mock_existing_curated_table, constants, job_params_inc):
    """Test INCREMENTAL process handles DELETE events with enrichment."""

    # DELETE event with proper schema
    schema = T.StructType([
        T.StructField("Keys", T.StructType([
            T.StructField("customerId", T.StructType([T.StructField("S", T.StringType())]))
        ])),
        T.StructField("NewImage", T.StructType([
            T.StructField("customerId", T.StructType([T.StructField("S", T.StringType())]))
        ]), nullable=True),
        T.StructField("eventName", T.StringType())
    ])

    data = [
        Row(
            Keys=Row(customerId=Row(S="CUST002")),
            NewImage=None,
            eventName="REMOVE"
        )
    ]
    df_with_deletes = spark.createDataFrame(data, schema=schema)

    job_config = JobConfig(
        params=job_params_inc,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params_inc.process_type)
    )

    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=df_with_deletes)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
    )
    sources = RawSources(catalog=catalog)

    # Mock catalog.get_dataframe_from_catalog for event_status comparison
    with patch.object(catalog, 'get_dataframe_from_catalog', return_value=mock_existing_curated_table):
        flatten_transformations = FlattenTransformations(
            job_config=job_config,
            sources=sources
        )
        df_flatten, df_keys, df_raw, record_count = flatten_transformations.get_flatten_table()

        assert record_count == 1
        assert "flag_event_status" in df_flatten.columns

        # Verify DELETE flag
        event_status = df_flatten.select("flag_event_status").first()[0]
        assert event_status == "DELETED"

        # ========== Business Transformations ==========
        dynamodb_table_transforms = DynamoDBTableTransformations(
            df_flatten=df_flatten,
            df_keys=df_keys,
            df_raw=df_raw,
            job_config=job_config,
            sources=sources
        )

        # Mock enrichment to return same df (skip actual enrichment logic in test)
        with patch('examples.co_delfos_clientes_prn_transformar_customerknowid_module.transform.transformations.enrich_deleted_records') as mock_enrich:
            mock_enrich.return_value = df_flatten  # Return df as-is
            df_transform = dynamodb_table_transforms.get_main_table()

            assert df_transform is not None
            assert "flag_event_status" in df_transform.columns

            # Verify enrichment was called
            mock_enrich.assert_called_once()


def test_incremental_empty_data(spark, glue_context, constants, job_params_inc):
    """Test INCREMENTAL pipeline handles empty data gracefully."""
    empty_df = spark.createDataFrame([], T.StructType([]))

    job_config = JobConfig(
        params=job_params_inc,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params_inc.process_type)
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

    assert result == (None, None, None, 0)
