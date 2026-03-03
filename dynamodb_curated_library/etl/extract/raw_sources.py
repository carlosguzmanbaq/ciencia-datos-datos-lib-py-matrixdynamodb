from dynamodb_curated_library.etl.extract.sources import Sources
from dynamodb_curated_library.models.storage_models import TableSourceConfig


class RawSources(Sources):
    """
    Extension of Sources class for DynamoDB ETL job.

    This class provides access to the single DynamoDB table source.
    """

    @property
    def table_source_config(self) -> TableSourceConfig:
        """
        Get the DynamoDB table source configuration.

        Returns:
            TableSourceConfig containing the table source configuration
        """
        return self.catalog.job_config.table_source
