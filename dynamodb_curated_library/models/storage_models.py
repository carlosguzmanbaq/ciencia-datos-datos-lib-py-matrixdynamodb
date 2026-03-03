from dataclasses import dataclass
from typing import Optional, TypedDict


class TableSourceConfig(TypedDict):
    """Type definition for DynamoDB table source configuration."""
    bucket: str
    prefix: str


@dataclass
class BucketNames:
    """Container for generated bucket names."""
    raw_bucket: Optional[str] = None
    curated_bucket: Optional[str] = None
    datalab_data_bucket: Optional[str] = None
    datalab_output_bucket: Optional[str] = None

    def __str__(self) -> str:
        """String representation for debugging and logging."""
        return (
            f"BucketNames(\n"
            f"  raw={self.raw_bucket}\n"
            f"  curated={self.curated_bucket}\n"
            f"  datalab_data={self.datalab_data_bucket}\n"
            f"  datalab_output={self.datalab_output_bucket}\n"
            f")"
        )


@dataclass
class TableCatalogNames:
    """Container for generated table paths."""
    curated_table_path: str
    curated_database: str

    def __str__(self) -> str:
        """String representation for debugging and logging."""
        return (
            f"TableCatalogNames(\n"
            f"  database={self.curated_database}\n"
            f"  table_path={self.curated_table_path}\n"
            f")"
        )
