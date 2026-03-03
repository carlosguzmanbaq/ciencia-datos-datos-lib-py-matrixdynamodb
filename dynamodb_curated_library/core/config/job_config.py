from typing import Dict, Any
from datetime import datetime

from dynamodb_curated_library.utils.banner import print_banner, print_job_info

from dynamodb_curated_library.core.config.constants import Constants
from dynamodb_curated_library.core.config.hudi.hudi_config_builder import HudiConfigBuilder
from dynamodb_curated_library.core.config.job_parameters import JobParameters
from dynamodb_curated_library.core.config.hudi.formatters import format_hudi_options
from dynamodb_curated_library.core.config.hudi.hudi_utils import HudiOptions
from dynamodb_curated_library.models.storage_models import BucketNames, TableCatalogNames, TableSourceConfig
from dynamodb_curated_library.models.dynamodb_models import DynamoDBExportConfig


class JobConfig:
    """
    Job configuration class with bucket and path generation logic.

    This class manages all configuration needed for a DynamoDB ETL job,
    including automatic generation of bucket names, database names, and paths
    based on conventions.
    """

    def __init__(
        self,
        params: JobParameters,
        constants: Constants,
        table_source: TableSourceConfig
    ):
        """
        Initialize JobConfig with validation.

        Args:
            account: AWS account ID
            env: Environment (dev, qc, pdn, etc.)
            process_date: Processing date in YYYY-MM-DD format (FULL) or YYYY-MM-DD HH format (INC)
            process_type: Process type (FULL or INC)
            constants: Constants configuration
            table_source: Dictionary with table source configuration
            is_datalab: Flag to indicate if running in datalab account (default: False)

        Raises:
            ValueError: If validation fails
        """
        self._validate_inputs(params.account, params.env, params.process_date, params.process_type, constants, table_source)

        self.account = params.account
        self.env = params.env
        self.process_date = params.process_date
        self.process_type = params.process_type
        self.constants = constants
        self.table_source = table_source
        self.is_datalab = params.is_datalab
        self.event_status = params.event_status
        self.log_level = params.log_level
        self.dynamodb_export = DynamoDBExportConfig(
            expody_name=constants.expody_name,
            data_product=constants.data_product,
            country_clan=constants.country_clan,
            process_type=params.process_type
        )
        # Generate derived properties
        date_format = "%Y-%m-%d %H" if params.process_type == "INC" else "%Y-%m-%d"
        self.start_process_date = datetime.strptime(params.process_date, date_format)

        # Print banner at job initialization
        print_banner()
        print_job_info(self)

    def _validate_inputs(
        self,
        account: str,
        env: str,
        process_date: str,
        process_type: str,
        constants: Constants,
        table_source: Dict[str, Any]
    ) -> None:
        """
        Validate input parameters.

        Raises:
            ValueError: If validation fails
        """
        validations = [
            (not account or not isinstance(account, str), "Account must be a non-empty string"),
            (not env or not isinstance(env, str), "Environment must be a non-empty string"),
            (not process_date, "Process date must be provided"),
            (not process_type or not isinstance(process_type, str), "Process type must be a non-empty string"),
            (not isinstance(constants, Constants), "Constants must be a Constants instance"),
            (not isinstance(table_source, dict), "Table source must be a dictionary")
        ]

        for condition, message in validations:
            if condition:
                raise ValueError(message)

        # Validate process_date format based on process_type
        date_format = "%Y-%m-%d %H" if process_type == "INC" else "%Y-%m-%d"
        expected_format = "YYYY-MM-DD HH" if process_type == "INC" else "YYYY-MM-DD"

        try:
            datetime.strptime(process_date, date_format)
        except ValueError as exc:
            raise ValueError(f"Process date must be in {expected_format} format") from exc

    @property
    def buckets(self) -> BucketNames:
        """Generate bucket names based on conventions."""
        base = f"{self.constants.country_mesh}-{self.constants.capacity}-{self.constants.domain}"
        suffix = f"{self.account}-{self.env}"

        if self.is_datalab:
            base = f"{self.constants.country_mesh}-datalab-{self.constants.domain}"
            return BucketNames(
                datalab_data_bucket=f"{base}-data-{suffix}".lower(),
                datalab_output_bucket=f"{base}-output-{suffix}".lower()
            )

        return BucketNames(
            raw_bucket=f"{base}-raw-{suffix}".lower(),
            curated_bucket=f"{base}-curated-{suffix}".lower()
        )


    @property
    def table_catalog_names(self) -> TableCatalogNames:
        """
        Generate table paths and database names based on conventions.

        Returns:
            TableCatalogNames instance with generated paths
        """
        if self.is_datalab:
            # Datalab: co_delfos_productos_datalab_pdn
            db_components = [
                self.constants.country_mesh,
                self.constants.capacity,
                self.constants.domain,
                "datalab",
                self.env
            ]
            curated_database = "_".join(db_components).lower()

            # Datalab: s3://co-datalab-productos-data-687973248987-pdn/...
            path_components = [
                f"s3://{self.buckets.datalab_data_bucket}",
                self.constants.subdomain,
                self.constants.catalog_name,
                self.table_name
            ]
            curated_table_path = "/".join(path_components).lower() + "/"
        else:
            # Mesh: co_delfos_productos_curated_pdn_rl
            db_components = [
                self.constants.country_mesh,
                self.constants.capacity,
                self.constants.domain,
                "curated",
                self.env,
                "rl"
            ]
            curated_database = "_".join(db_components).lower()

            # Mesh: s3://co-delfos-productos-curated-710976466405-pdn/...
            path_components = [
                f"s3://{self.buckets.curated_bucket}",
                self.constants.subdomain,
                self.constants.catalog_name,
                self.table_name
            ]
            curated_table_path = "/".join(path_components).lower() + "/"

        return TableCatalogNames(
            curated_table_path=curated_table_path,
            curated_database=curated_database
        )

    @property
    def process_day_parts_str(self) -> str:
        return self.start_process_date.strftime("%Y-%m-%d").split('-')

    @property
    def table_name(self) -> str:
        """Generate table name based on conventions."""
        components = [
            self.constants.country_mesh,
            self.constants.prefix_name,
            self.constants.data_product
        ]
        return "_".join(components).lower()

    @property
    def hudi_options(self) -> HudiOptions:
        """Get Hudi configuration options using HudiConfigBuilder."""
        builder = HudiConfigBuilder(
            constants=self.constants,
            table_name=self.table_name,
            curated_database=self.table_catalog_names.curated_database,
            custom_options=self.constants.custom_hudi_options
        )
        return builder.build()

    def __str__(self) -> str:
        """String representation for debugging and logging."""
        hudi_formatted = format_hudi_options(self.hudi_options)
        return (
            f"JobConfig(\n"
            f"  account={self.account}\n"
            f"  env={self.env}\n"
            f"  process_date={self.process_date}\n"
            f"  process_type={self.process_type}\n"
            f"  is_datalab={self.is_datalab}\n"
            f"  table_name={self.table_name}\n"
            f"\n  Buckets:\n{self.buckets}\n"
            f"\n  Table Catalog:\n{self.table_catalog_names}\n"
            f"\n  DynamoDB Export:\n{self.dynamodb_export}\n"
            f"\n  Hudi Options:\n{hudi_formatted}\n"
            f")\n"
        )
