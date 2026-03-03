from dynamodb_curated_library.core.config.constants import Constants
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems import metadata

_DOMAIN: str = "productos"
_SUBDOMAIN: str = "srf"
_EXPODY_NAME: str = "interno_expody"
_PREFIX_NAME: str = "dynamodb"
_DATA_PRODUCT: str = "surprise_gift_redeems"
_CAPACITY: str = "delfos"
_COUNTRY_MESH: str = "co"
_COUNTRY_CLAN: str = "co"
_CATALOG_NAME: str = "table-catalog"
_INSERT_MODE: str = "append"
_PRIMARY_KEY: str = ["clientId", "tstamp"]
_PRECOMBINE_KEY: str = "job_process_date"
_METADATA_MODULE = metadata
_PACKAGE_NAME = "co_delfos_productos_srf_transformar_surprise_gift_redeems"
_PARTITION_FIELD: str = "tstamp"
_CUSTOM_HUDI_OPTIONS = {
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",  # "upsert", "insert", "bulk_insert", "delete"
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
    "hoodie.datasource.hive_sync.sync_comment": "true",
    # "hoodie.datasource.write.payload.class": None # elimnar configuracion de Hudie por si no se usa
}


def building_constants() -> Constants:
    return Constants.build_constants({
        "domain": _DOMAIN,
        "subdomain": _SUBDOMAIN,
        "expody_name": _EXPODY_NAME,
        "prefix_name": _PREFIX_NAME,
        "data_product": _DATA_PRODUCT,
        "capacity": _CAPACITY,
        "country_mesh": _COUNTRY_MESH,
        "country_clan": _COUNTRY_CLAN,
        "catalog_name": _CATALOG_NAME,
        "insert_mode": _INSERT_MODE,
        "primary_key": _PRIMARY_KEY,
        "precombine_key": _PRECOMBINE_KEY,
        "metadata_module": _METADATA_MODULE,
        "package_name": _PACKAGE_NAME,
        "partition_field": _PARTITION_FIELD,
        "custom_hudi_options": _CUSTOM_HUDI_OPTIONS,
    })
