from unittest.mock import Mock, patch

import json
import pytest
from pyspark.sql import types as T


from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.etl.extract.catalog import Catalog
from dynamodb_curated_library.etl.extract.raw_sources import RawSources
from dynamodb_curated_library.etl.transform.transformations import FlattenTransformations
from dynamodb_curated_library.metadata.metadata import MetadataConfig

from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.config.sources_dictionary import get_raw_dynamodb_source
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.transform.transformations import DynamoDBTableTransformations


@pytest.fixture(name="mock_dynamodb_incremental_data")
def mock_dynamodb_incremental_data_fixture(spark):
    """Create mock DynamoDB INCREMENTAL export data."""
    data = [
        {
            "Keys": {
                "clientId": {"S": "CLIENT001"},
                "tstamp": {"N": "1705320000000"}
            },
            "NewImage": {
                "clientId": {"S": "CLIENT001"},
                "tstamp": {"N": "1705320000000"},
                "documentNumber": {"S": "123456789"},
                "documentType": {"S": "CC"},
                "giftCode": {"S": "GIFT123"},
                "merchantId": {"S": "MERCHANT001"},
                "phoneNumber": {"S": "+573001234567"},
                "transactionId": {"S": "TXN001"},
                "value": {"N": "50000"}
            },
            "eventName": "MODIFY"
        },
        {
            "Keys": {
                "clientId": {"S": "CLIENT003"},
                "tstamp": {"N": "1705326000000"}
            },
            "NewImage": {
                "clientId": {"S": "CLIENT003"},
                "tstamp": {"N": "1705326000000"},
                "documentNumber": {"S": "111222333"},
                "documentType": {"S": "CC"},
                "giftCode": {"S": "GIFT789"},
                "merchantId": {"S": "MERCHANT003"},
                "phoneNumber": {"S": "+573009999999"},
                "transactionId": {"S": "TXN003"},
                "value": {"S": "100000"}
            },
            "eventName": "INSERT"
        }
    ]
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(d) for d in data]))


def test_incremental_etl_pipeline(glue_context, mock_dynamodb_incremental_data, constants, job_params_inc):
    """Test complete ETL pipeline for INCREMENTAL process."""

    # ========== Configuration ==========
    job_config = JobConfig(
        params=job_params_inc,
        constants=constants,
        table_source=get_raw_dynamodb_source(job_params_inc.process_type)
    )

    assert job_config.process_type == "INC"

    # ========== Extract ==========
    glue_context.create_data_frame = Mock()
    glue_context.create_data_frame.from_options = Mock(return_value=mock_dynamodb_incremental_data)

    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
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
    assert "client_id" in df_flatten.columns
    assert "tstamp" in df_flatten.columns

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
        assert "job_process_type" in df_final.columns


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
