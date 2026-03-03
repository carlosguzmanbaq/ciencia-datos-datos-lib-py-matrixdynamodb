from unittest.mock import Mock, patch

import json
import pytest
from pyspark.sql import types as T


from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.extract.catalog import Catalog
from dynamodb_curated_library.etl.extract.raw_sources import RawSources
from dynamodb_curated_library.etl.transform.transformations import FlattenTransformations
from dynamodb_curated_library.metadata.metadata import MetadataConfig

from examples.co_delfos_clientes_prn_transformar_customerknowid_module.config.sources_dictionary import get_raw_dynamodb_source
from examples.co_delfos_clientes_prn_transformar_customerknowid_module.schemas.dynamodb_customer_know_id_update_module import schema_full
from examples.co_delfos_clientes_prn_transformar_customerknowid_module.transform.transformations import DynamoDBTableTransformations


@pytest.fixture(name="mock_dynamodb_full_data")
def mock_dynamodb_full_data_fixture(spark):
    """Create mock DynamoDB FULL export data."""
    data = [
        {
            "Item": {
                "customerId": {"S": "CUST001"},
                "email": {"S": "john@example.com"},
                "firstName": {"S": "John"},
                "phoneNumber": {"S": "+573001234567"},
                "creationDate": {"S": "2024-01-15T10:00:00Z"},
                "updateDate": {"S": "2024-01-15T10:00:00Z"},
                "owner": {"S": "system"},
                "risk": {"S": "LOW"},
                "notified": {"S": "true"},
                "notificationDate": {"S": "2024-01-15T10:30:00Z"},
                "products": {
                    "M": {
                        "credits": {"BOOL": True},
                        "remittances": {"BOOL": True},
                        "paypal": {"BOOL": False},
                        "savingsAccount": {"BOOL": False}
                    }
                },
                "notifications": {
                    "L": [
                        {
                            "M": {
                                "type": {"S": "EMAIL"},
                                "notified": {"BOOL": True},
                                "notificationDate": {"S": "2024-01-15T10:30:00Z"},
                                "messageId": {"S": "MSG001"},
                                "notifiedDate": {"S": "2024-01-15T10:35:00Z"}
                            }
                        }
                    ]
                }
            }
        },
        {
            "Item": {
                "customerId": {"S": "CUST002"},
                "email": {"S": "jane@example.com"},
                "firstName": {"S": "Jane"},
                "phoneNumber": {"S": "+573007654321"},
                "creationDate": {"S": "2024-01-15T11:00:00Z"},
                "updateDate": {"S": "2024-01-15T11:00:00Z"},
                "owner": {"S": "system"},
                "risk": {"S": "MEDIUM"},
                "notified": {"S": "false"},
                "products": {
                    "M": {
                        "credits": {"BOOL": False},
                        "remittances": {"BOOL": True},
                        "paypal": {"BOOL": True},
                        "savingsAccount": {"BOOL": True}
                    }
                }
            }
        }
    ]
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(d) for d in data]))


def test_full_etl_pipeline_with_event_status(glue_context, mock_dynamodb_full_data, constants, job_params_full):
    """Test complete ETL pipeline for FULL process with event_status enabled."""

    # ========== Configuration ==========
    job_config = JobConfig(
        params=job_params_full,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params_full.process_type)
    )

    assert job_config.process_type == "FULL"
    assert job_config.event_status is True

    # ========== Extract ==========
    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=mock_dynamodb_full_data)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
        schema=schema_full,
    )
    sources = RawSources(catalog=catalog)

    assert sources.dynamodb_table.count() == 2

    # ========== Transform ==========
    flatten_transformations = FlattenTransformations(
        job_config=job_config,
        sources=sources
    )
    df_flatten, df_keys, df_raw, record_count = flatten_transformations.get_flatten_table()

    assert record_count == 2
    assert "customer_id" in df_flatten.columns
    assert "flag_event_status" in df_flatten.columns

    # Verify all records are INSERTED for FULL
    event_statuses = [row.flag_event_status for row in df_flatten.select("flag_event_status").collect()]
    assert all(status == "INSERTED" for status in event_statuses)

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
    assert df_transform.count() == 2

    # ========== Metadata ==========
    with patch('dynamodb_curated_library.metadata.metadata.MetadataConfig.load_metadata') as mock_load:
        mock_load.return_value = {}

        metadata_config = MetadataConfig(
            file_name=job_config.table_name,
            constants=constants,
            target_module="metadata"
        )
        df_final = metadata_config.add_metadata_columns(df=df_transform, job_config=job_config)

        assert "momento_ingestion" in df_final.columns
        assert "job_process_date" in df_final.columns
        assert "flag_event_status" in df_final.columns


def test_full_etl_pipeline_empty_data(spark, glue_context, constants, job_params_full):
    """Test ETL pipeline handles empty data gracefully."""
    empty_df = spark.createDataFrame([], T.StructType([]))

    job_config = JobConfig(
        params=job_params_full,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params_full.process_type)
    )

    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=empty_df)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
        schema=schema_full,
    )
    sources = RawSources(catalog=catalog)

    flatten_transformations = FlattenTransformations(
        job_config=job_config,
        sources=sources
    )
    result = flatten_transformations.get_flatten_table()

    assert result == (None, None, None, 0)
