from unittest.mock import Mock
import pytest

from dynamodb_curated_library.core.config.job_parameters import JobParameters
from examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.config.constans import building_constants


@pytest.fixture(name="constants")
def constants_fixture():
    """Create test constants."""
    return building_constants()


@pytest.fixture(name="job_params_full")
def job_params_full_fixture():
    """Create test job parameters for FULL process."""
    params = Mock(spec=JobParameters)
    params.process_date = "2024-01-15"
    params.account = "123456789"
    params.env = "dev"
    params.process_type = "FULL"
    params.is_datalab = False
    params.log_level = "INFO"
    params.event_status = False
    return params


@pytest.fixture(name="job_params_inc")
def job_params_inc_fixture():
    """Create test job parameters for INCREMENTAL process."""
    params = Mock(spec=JobParameters)
    params.process_date = "2024-01-15 14"
    params.account = "123456789"
    params.env = "dev"
    params.process_type = "INC"
    params.is_datalab = False
    params.log_level = "INFO"
    params.event_status = False
    return params
