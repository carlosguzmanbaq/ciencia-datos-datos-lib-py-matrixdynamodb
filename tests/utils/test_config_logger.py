from unittest.mock import Mock

from dynamodb_curated_library.utils.config_logger import log_configuration


def test_log_configuration_prints_all_sections(capsys):
    """Test log_configuration prints all configuration sections."""
    mock_constants = Mock()
    mock_constants.__str__ = Mock(return_value="Constants config")

    mock_params = Mock()
    mock_params.__str__ = Mock(return_value="Job parameters")

    mock_job_config = Mock()
    mock_job_config.__str__ = Mock(return_value="Job config")

    log_configuration(mock_constants, mock_params, mock_job_config)

    captured = capsys.readouterr()

    assert "=== CONSTANTS ===" in captured.out
    assert "Constants config" in captured.out
    assert "=== PARAMETERS ===" in captured.out
    assert "Job parameters" in captured.out
    assert "=== JOB CONFIG ===" in captured.out
    assert "Job config" in captured.out
