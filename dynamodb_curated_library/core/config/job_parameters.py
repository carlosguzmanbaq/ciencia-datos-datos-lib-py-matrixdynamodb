import sys
from dataclasses import dataclass
from typing import List, Optional
from awsglue.utils import getResolvedOptions
from awsglue.utils import GlueArgumentError

@dataclass
class JobParameters:
    """Job parameters configuration."""
    account: str
    env: str
    process_date: str
    process_type: str
    is_datalab: bool = False
    log_level: str = "INFO"
    event_status: bool = False

    @staticmethod
    def normalize_boolean(value) -> bool:
        """
        Normalize boolean value from various input types.

        Args:
            value: Input value (bool, str)

        Returns:
            Normalized boolean value

        Raises:
            ValueError: If value cannot be converted to boolean
        """
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized == "true":
                return True
            if normalized == "false" or normalized == "":
                return False
            raise ValueError(f"Invalid boolean string: '{value}'. Expected: true/false")

        raise ValueError(f"Cannot convert {type(value).__name__} to boolean")

    @classmethod
    def from_args(cls, required_params: Optional[List[str]] = None) -> 'JobParameters':
        """Create JobParameters from command line arguments."""
        # Required parameters
        required = required_params or ["ACCOUNT", "ENV", "PROCESS_DATE", "PROCESS_TYPE"]
        args = getResolvedOptions(sys.argv, required)

        # Optional parameters - handle each separately
        log_level = "INFO"
        try:
            log_level_args = getResolvedOptions(sys.argv, ["LOG_LEVEL"])
            log_level = log_level_args.get("LOG_LEVEL", "INFO").upper()
        except GlueArgumentError:
            pass

        event_status = False
        try:
            es_args = getResolvedOptions(sys.argv, ["EVENT_STATUS"])
            event_status = cls.normalize_boolean(es_args.get("EVENT_STATUS", False))
        except GlueArgumentError:
            pass

        is_datalab = False
        try:
            datalab_args = getResolvedOptions(sys.argv, ["IS_DATALAB"])
            is_datalab = cls.normalize_boolean(datalab_args.get("IS_DATALAB", False))
        except GlueArgumentError:
            pass

        return cls(
            account=args.get("ACCOUNT", ""),
            env=args.get("ENV", ""),
            process_date=args.get("PROCESS_DATE", ""),
            process_type=args.get("PROCESS_TYPE", ""),
            is_datalab=is_datalab,
            log_level=log_level,
            event_status=event_status
        )

    def __str__(self) -> str:
        """String representation for debugging and logging."""
        return (
            f"JobParameters(\n"
            f"  account={self.account}\n"
            f"  env={self.env}\n"
            f"  process_date={self.process_date}\n"
            f"  process_type={self.process_type}\n"
            f"  is_datalab={self.is_datalab}\n"
            f"  log_level={self.log_level}\n"
            f")"
        )
