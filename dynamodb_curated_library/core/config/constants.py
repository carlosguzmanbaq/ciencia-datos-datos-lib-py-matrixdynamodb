from dataclasses import dataclass, field
from typing import TypedDict, Dict, Any, List, Union
from typing_extensions import NotRequired


class ConstantsDict(TypedDict):
    """Type definition for Constants configuration dictionary."""
    domain: str
    subdomain: str
    prefix_name: str
    data_product: str
    country_mesh: str
    primary_key: Union[str, List[str]]
    metadata_module: any
    package_name: str
    expody_name: NotRequired[str]
    capacity: NotRequired[str]
    country_clan: NotRequired[str]
    catalog_name: NotRequired[str]
    insert_mode: NotRequired[str]
    precombine_key: NotRequired[str]
    partition_field: NotRequired[str]
    custom_hudi_options: NotRequired[Dict[str, Any]]


@dataclass
class Constants:
    """
    Constants configuration

    This class holds domain-specific configuration values that define
    the data product structure and processing behavior.
    """
    domain: str
    subdomain: str
    prefix_name: str
    data_product: str
    country_mesh: str
    primary_key: Union[str, List[str]]
    metadata_module: any
    package_name: str
    partition_field: str = ""
    expody_name: str = "interno_expody"
    capacity: str = "delfos"
    country_clan: str = "co"
    catalog_name: str = "table-catalog"
    insert_mode: str = "append"
    precombine_key: str = "job_process_date"
    custom_hudi_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Normalize primary_key to always be a list."""
        if isinstance(self.primary_key, str):
            self.primary_key = [self.primary_key]

    @property
    def primary_keys(self) -> List[str]:
        """Get primary keys as a list."""
        return self.primary_key if isinstance(self.primary_key, list) else [self.primary_key]

    @staticmethod
    def build_constants(config: ConstantsDict) -> 'Constants':
        # Required fields
        required_fields = ["domain", "subdomain", "prefix_name", "data_product", "country_mesh", "primary_key"]

        # Validation
        missing = [field for field in required_fields if field not in config or config[field] is None]
        if missing:
            raise ValueError(f"Missing fields: {', '.join(missing)}")

        return Constants(**config)

    def __str__(self) -> str: # pragma: no cover
        """String representation for debugging and logging."""
        return (
            f"Constants(\n"
            f"  domain={self.domain}\n"
            f"  subdomain={self.subdomain}\n"
            f"  data_product={self.data_product}\n"
            f"  country_mesh={self.country_mesh}\n"
            f"  primary_keys={self.primary_keys}\n"
            f"  capacity={self.capacity}\n"
            f")"
        )
