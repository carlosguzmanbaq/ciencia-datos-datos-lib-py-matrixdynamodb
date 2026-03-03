from typing import Dict, Any, Optional

from dynamodb_curated_library.core.config.constants import Constants
from dynamodb_curated_library.core.config.hudi.hudi_utils import HudiOptions
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import to_snake_case


class HudiConfigBuilder:
    """
    Builder class for Hudi configuration options.

    This class is responsible for generating Hudi configuration based on
    job parameters and constants, with support for custom options.
    """

    def __init__(
        self,
        constants: Constants,
        table_name: str,
        curated_database: str,
        custom_options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize HudiConfigBuilder.

        Args:
            constants: Constants configuration
            table_name: Name of the table
            curated_database: Name of the curated database
            custom_options: Optional custom Hudi configuration options
        """
        self.constants = constants
        self.table_name = table_name
        self.curated_database = curated_database
        self.custom_options = custom_options or {}

    def build(self) -> HudiOptions:
        """
        Build Hudi configuration options.

        Returns:
            HudiOptions dictionary with all configuration
        """
        # Base configuration (common to all scenarios)
        config: HudiOptions = {
            "hoodie.table.name": self.table_name,
            "hoodie.datasource.write.recordkey.field": ",".join(to_snake_case(key) for key in self.constants.primary_key),
            "hoodie.datasource.write.precombine.field": self.constants.precombine_key,
            "hoodie.metadata.enable": "false",
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database": self.curated_database,
            "hoodie.datasource.hive_sync.table": self.table_name,
            "hoodie.datasource.hive_sync.support_timestamp": "true",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.mode": "hms",
            "mode": self.constants.insert_mode,
        }

        self._add_partition_config(config)
        self._merge_custom_options(config)

        return config

    def _add_partition_config(self, config: HudiOptions) -> None:
        """
        Add partition configuration if partition field is defined.

        Args:
            config: Hudi configuration dictionary to modify
        """
        if hasattr(self.constants, 'partition_field') and self.constants.partition_field:
            config["hoodie.datasource.hive_sync.partition_fields"] = "year,month,day"
            config["hoodie.datasource.write.partitionpath.field"] = "year,month,day"

    def _merge_custom_options(self, config: HudiOptions) -> None:
        """
        Merge custom options into the configuration.

        Custom options can override default values or remove them by setting value to None.

        Args:
            config: Hudi configuration dictionary to modify
        """
        if self.custom_options:
            for key, value in self.custom_options.items():
                if value is None:
                    config.pop(key, None)  # Remove key if value is None
                else:
                    config[key] = value  # Add or override
