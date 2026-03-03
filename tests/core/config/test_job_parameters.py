from unittest.mock import patch

import pytest
from awsglue.utils import GlueArgumentError

from dynamodb_curated_library.core.config.job_parameters import JobParameters


class TestNormalizeBoolean:
    """Tests for normalize_boolean method."""

    def test_normalize_boolean_with_true_bool(self):
        """Test normalize_boolean with True boolean."""
        assert JobParameters.normalize_boolean(True) is True

    def test_normalize_boolean_with_false_bool(self):
        """Test normalize_boolean with False boolean."""
        assert JobParameters.normalize_boolean(False) is False

    def test_normalize_boolean_with_true_string(self):
        """Test normalize_boolean with 'true' string."""
        assert JobParameters.normalize_boolean("true") is True
        assert JobParameters.normalize_boolean("True") is True
        assert JobParameters.normalize_boolean("TRUE") is True
        assert JobParameters.normalize_boolean("  true  ") is True

    def test_normalize_boolean_with_false_string(self):
        """Test normalize_boolean with 'false' string."""
        assert JobParameters.normalize_boolean("false") is False
        assert JobParameters.normalize_boolean("False") is False
        assert JobParameters.normalize_boolean("FALSE") is False
        assert JobParameters.normalize_boolean("  false  ") is False

    def test_normalize_boolean_with_empty_string(self):
        """Test normalize_boolean with empty string."""
        assert JobParameters.normalize_boolean("") is False
        assert JobParameters.normalize_boolean("  ") is False

    def test_normalize_boolean_raises_error_for_invalid_string(self):
        """Test normalize_boolean raises ValueError for invalid string."""
        with pytest.raises(ValueError, match="Invalid boolean string"):
            JobParameters.normalize_boolean("yes")

        with pytest.raises(ValueError, match="Invalid boolean string"):
            JobParameters.normalize_boolean("1")

    def test_normalize_boolean_raises_error_for_invalid_type(self):
        """Test normalize_boolean raises ValueError for invalid type."""
        with pytest.raises(ValueError, match="Cannot convert int to boolean"):
            JobParameters.normalize_boolean(1)

        with pytest.raises(ValueError, match="Cannot convert list to boolean"):
            JobParameters.normalize_boolean([])


class TestFromArgs:
    """Tests for from_args class method."""

    @patch('dynamodb_curated_library.core.config.job_parameters.getResolvedOptions')
    def test_from_args_with_required_params_only(self, mock_get_resolved):
        """Test from_args with only required parameters."""
        mock_get_resolved.side_effect = [
            {"ACCOUNT": "123456", "ENV": "dev", "PROCESS_DATE": "2024-01-15", "PROCESS_TYPE": "FULL"},
            GlueArgumentError("LOG_LEVEL not found"),
            GlueArgumentError("EVENT_STATUS not found"),
            GlueArgumentError("IS_DATALAB not found")
        ]

        params = JobParameters.from_args()

        assert params.account == "123456"
        assert params.env == "dev"
        assert params.process_date == "2024-01-15"
        assert params.process_type == "FULL"
        assert params.log_level == "INFO"
        assert params.event_status is False
        assert params.is_datalab is False

    @patch('dynamodb_curated_library.core.config.job_parameters.getResolvedOptions')
    def test_from_args_with_all_params(self, mock_get_resolved):
        """Test from_args with all parameters."""
        mock_get_resolved.side_effect = [
            {"ACCOUNT": "123456", "ENV": "pdn", "PROCESS_DATE": "2024-01-15", "PROCESS_TYPE": "INC"},
            {"LOG_LEVEL": "debug"},
            {"EVENT_STATUS": "true"},
            {"IS_DATALAB": "true"}
        ]

        params = JobParameters.from_args()

        assert params.account == "123456"
        assert params.env == "pdn"
        assert params.process_date == "2024-01-15"
        assert params.process_type == "INC"
        assert params.log_level == "DEBUG"
        assert params.event_status is True
        assert params.is_datalab is True

    @patch('dynamodb_curated_library.core.config.job_parameters.getResolvedOptions')
    def test_from_args_with_custom_required_params(self, mock_get_resolved):
        """Test from_args with custom required parameters."""
        mock_get_resolved.side_effect = [
            {"CUSTOM_PARAM": "value"},
            GlueArgumentError("LOG_LEVEL not found"),
            GlueArgumentError("EVENT_STATUS not found"),
            GlueArgumentError("IS_DATALAB not found")
        ]

        params = JobParameters.from_args(required_params=["CUSTOM_PARAM"])

        assert params.account == ""
        assert params.env == ""

    @patch('dynamodb_curated_library.core.config.job_parameters.getResolvedOptions')
    def test_from_args_normalizes_log_level_to_uppercase(self, mock_get_resolved):
        """Test from_args converts log level to uppercase."""
        mock_get_resolved.side_effect = [
            {"ACCOUNT": "123", "ENV": "dev", "PROCESS_DATE": "2024-01-15", "PROCESS_TYPE": "FULL"},
            {"LOG_LEVEL": "debug"},
            GlueArgumentError("EVENT_STATUS not found"),
            GlueArgumentError("IS_DATALAB not found")
        ]

        params = JobParameters.from_args()

        assert params.log_level == "DEBUG"


class TestJobParameters:
    """Tests for JobParameters dataclass."""

    def test_job_parameters_creation(self):
        """Test JobParameters can be created with required fields."""
        params = JobParameters(
            account="123456",
            env="dev",
            process_date="2024-01-15",
            process_type="FULL"
        )

        assert params.account == "123456"
        assert params.env == "dev"
        assert params.process_date == "2024-01-15"
        assert params.process_type == "FULL"
        assert params.is_datalab is False
        assert params.log_level == "INFO"
        assert params.event_status is False

    def test_job_parameters_with_all_fields(self):
        """Test JobParameters with all fields specified."""
        params = JobParameters(
            account="123456",
            env="pdn",
            process_date="2024-01-15",
            process_type="INC",
            is_datalab=True,
            log_level="DEBUG",
            event_status=True
        )

        assert params.account == "123456"
        assert params.env == "pdn"
        assert params.process_date == "2024-01-15"
        assert params.process_type == "INC"
        assert params.is_datalab is True
        assert params.log_level == "DEBUG"
        assert params.event_status is True

    def test_job_parameters_str_representation(self):
        """Test JobParameters string representation."""
        params = JobParameters(
            account="123456",
            env="dev",
            process_date="2024-01-15",
            process_type="FULL"
        )

        str_repr = str(params)

        assert "JobParameters(" in str_repr
        assert "account=123456" in str_repr
        assert "env=dev" in str_repr
        assert "process_date=2024-01-15" in str_repr
        assert "process_type=FULL" in str_repr
        assert "is_datalab=False" in str_repr
        assert "log_level=INFO" in str_repr
