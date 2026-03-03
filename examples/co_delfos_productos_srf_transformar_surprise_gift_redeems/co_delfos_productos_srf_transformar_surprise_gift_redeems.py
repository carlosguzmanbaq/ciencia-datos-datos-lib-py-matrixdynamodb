import logging
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

from dynamodb_curated_library.core.config.constants import Constants
from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.core.config.job_parameters import JobParameters

from dynamodb_curated_library.dev_utils.local_env import IS_LOCAL_ENV, DevGlueContext, setup_colombian_locale
from dynamodb_curated_library.etl.load.save import save_hudi
from dynamodb_curated_library.metadata.metadata import MetadataConfig
from dynamodb_curated_library.utils.config_logger import log_configuration
from dynamodb_curated_library.utils.logging_config import configure_logging

from dynamodb_curated_library.etl.extract.catalog import Catalog
from dynamodb_curated_library.etl.extract.raw_sources import RawSources
from dynamodb_curated_library.etl.transform.transformations import FlattenTransformations

from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.config.spark_config import spark_config
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.config.constans import building_constants
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.config.sources_dictionary import get_raw_dynamodb_source
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.schemas.dynamodb_surprise_gift_redeems import schema_full
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.transform.transformations import DynamoDBTableTransformations

logger = logging.getLogger(__name__)


def main(constants: Constants, params: JobParameters, glue_context: GlueContext):

    # ========== Configuration ==========
    job_config = JobConfig(
        params=params,
        constants=constants,
        table_source=get_raw_dynamodb_source(params.process_type)
    )

    if logger.isEnabledFor(logging.DEBUG):
        log_configuration(constants, params, job_config)

    # ========== Extract ==========
    catalog = Catalog(
        glue_context=glue_context,
        job_config=job_config,
        #create_full_schema=True,
        schema=schema_full,
    )
    sources = RawSources(catalog=catalog)

    # ========== Transform ==========
    flatten_transformations = FlattenTransformations(
        job_config=job_config,
        sources=sources,
        # max_depth=3,
        # columns_as_json=["state.M.orderSteps.L"]
    )
    df_flatten, df_keys, df_raw, record_count = flatten_transformations.get_flatten_table()
    if record_count == 0:
        logger.info(
            "No records to process - Process date: %s, Process type: %s, Table: %s",
            job_config.process_date,
            job_config.process_type,
            job_config.table_name
        )
        return

    dynamodb_table_transforms = DynamoDBTableTransformations(
        df_flatten=df_flatten,
        df_keys=df_keys,
        df_raw=df_raw,
        job_config=job_config,
        sources=sources,
        # create_final_schema=True
    )
    df_transform = dynamodb_table_transforms.get_main_table()

    # ========== Metadata ==========
    metadata_config = MetadataConfig(
       file_name=job_config.table_name,
       constants=constants,
       target_module="metadata"
    )
    df_final = metadata_config.add_metadata_columns(df=df_transform, job_config=job_config)
    logger.info("Amount of data to write: %s", df_transform.count())

    logger.debug("\n\nFinal DF Schema:")
    if logger.isEnabledFor(logging.DEBUG):
        df_final.printSchema()

    # ========== Load ==========
    save_hudi(
        df=df_final,
        hudi_options=job_config.hudi_options,
        path=job_config.table_catalog_names.curated_table_path,
        mode=job_config.constants.insert_mode,
    )


if __name__ == "__main__":

    _params: JobParameters = JobParameters.from_args()
    _constants: Constants = building_constants()

    # Configure logging
    configure_logging(_params.log_level)

    sparkBuilder: SparkSession.Builder = SparkSession.builder
    _spark: SparkSession = (
        sparkBuilder
        .appName(_constants.data_product)
        .enableHiveSupport()
        .config(conf=spark_config)
        .getOrCreate()
    )

    if IS_LOCAL_ENV:
        # pylint: disable = W0212
        setup_colombian_locale(_spark._jvm)
        _glue_context = DevGlueContext(spark=_spark)
        logger.info("Running in local mode")
    else:
        _glue_context = GlueContext(sparkContext=_spark.sparkContext)
        logger.info("Running in cloud mode")

    main(_constants, _params, _glue_context)
