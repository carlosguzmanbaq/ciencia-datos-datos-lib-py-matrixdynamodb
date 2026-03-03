from dataclasses import dataclass
from typing import Literal

ProcessType = Literal["INC", "FULL"]

@dataclass
class DynamoDBExportConfig:
    """Configuración de exportación para trabajos de DynamoDB."""

    expody_name: str
    data_product: str
    country_clan: str
    process_type: ProcessType

    @property
    def export_folder_name(self) -> str:
        "Generates the name of the DynamoDB export folder."
        return f"{self.country_clan}_{self.expody_name}_{self.data_product}".lower()

    @property
    def dynamodb_process_type_folder(self) -> str:
        """Determine el tipo de carpeta según el proceso de DynamoDB."""
        return "incremental" if self.is_incremental else "full"

    @property
    def is_incremental(self) -> bool:
        """Check if the DynamoDB process is incremental."""
        return self.process_type == "INC"

    def __str__(self) -> str:
        """String representation for debugging and logging."""
        return (
            f"DynamoDBExportConfig(\n"
            f"  expody_name={self.expody_name}\n"
            f"  data_product={self.data_product}\n"
            f"  process_type={self.process_type}\n"
            f"  export_folder={self.export_folder_name}\n"
            f")"
        )
