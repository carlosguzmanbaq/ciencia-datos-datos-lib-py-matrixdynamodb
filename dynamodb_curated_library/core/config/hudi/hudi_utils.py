from typing import TypedDict
from typing_extensions import NotRequired


HudiOptions = TypedDict(
    'HudiOptions', {
        "hoodie.table.name": str,
        "hoodie.datasource.write.storage.type": str,
        "hoodie.datasource.write.operation": str,
        "hoodie.datasource.write.recordkey.field": str,
        "hoodie.datasource.write.precombine.field": str,
        "hoodie.datasource.write.hive_style_partitioning": str,
        "hoodie.metadata.enable": str,
        "hoodie.datasource.hive_sync.enable": str,
        "hoodie.datasource.hive_sync.database": str,
        "hoodie.datasource.hive_sync.table": str,
        "hoodie.datasource.hive_sync.partition_extractor_class": str,
        "hoodie.datasource.write.payload.class": str,
        "hoodie.datasource.hive_sync.use_jdbc": str,
        "hoodie.datasource.hive_sync.mode": str,
        "hoodie.datasource.hive_sync.partition_fields": NotRequired[str],
        "hoodie.datasource.write.partitionpath.field": NotRequired[str],
        "mode": str,
    },
    total=False
)
