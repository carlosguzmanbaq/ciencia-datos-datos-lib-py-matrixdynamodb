import json
from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.models.storage_models import TableSourceConfig
from dynamodb_curated_library.models.table import S3File


class ConfigTable:
    """
    Configuration manager for DynamoDB table source with dynamic placeholder replacement.

    This class processes the table source configuration by replacing placeholders with actual
    values from the job configuration, enabling dynamic path generation for S3 locations
    based on processing dates and environment settings.

    Attributes:
        source_config (TableSourceConfig): Table configuration with resolved placeholders.
    """

    def __init__(self, job_config: JobConfig) -> None:
        year, month, day = job_config.process_day_parts_str
        hour = job_config.start_process_date.strftime("%H")

        # Select appropriate bucket based on environment
        bucket = job_config.buckets.datalab_data_bucket if job_config.is_datalab else job_config.buckets.raw_bucket

        json_str = (
            json.dumps(job_config.table_source)
            .replace('{raw_bucket}', bucket)
            .replace('{export_folder_name}', job_config.dynamodb_export.export_folder_name)
            .replace('{domain}', job_config.constants.domain)
            .replace('{country}', job_config.constants.country_clan)
            .replace('{current_subdomain}', job_config.constants.subdomain)
            .replace('{current_dynamodb_process_type_folder}', job_config.dynamodb_export.dynamodb_process_type_folder)
            .replace('{current_account}', job_config.account.lower())
            .replace('{current_env}', job_config.env.lower())
            .replace('{process_year}', year)
            .replace('{process_month}', month)
            .replace('{process_day}', day)
            .replace('{process_hour}', hour)
        )

        self.source_config: TableSourceConfig = json.loads(json_str)

    def get_source_options(self) -> S3File:
        """
        Create an S3File object with resolved configuration for the DynamoDB table.

        Returns:
            S3File: An S3File object with bucket, prefix, origin, and format information.
        """
        return S3File(**self.source_config)
