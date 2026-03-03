
from pyspark.sql import DataFrame
from dynamodb_curated_library.etl.extract.catalog import Catalog


class Sources:
    """
    Base class for managing DynamoDB data source extraction.

    This class handles the initialization of the single DynamoDB table source.

    Attributes:
        catalog: Catalog instance containing job configuration and table access methods
        dynamodb_table: DataFrame containing the DynamoDB table data
    """
    catalog: Catalog
    dynamodb_table: DataFrame

    def __init__(self, catalog: Catalog):
        """
        Initialize Sources with catalog.

        Loads the DynamoDB table source from the catalog.

        Args:
            catalog: Catalog instance containing job configuration and table access
        """
        self.catalog = catalog
        self.dynamodb_table = catalog.get_table()
