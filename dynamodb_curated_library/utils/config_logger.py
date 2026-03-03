"""Utility functions for logging configuration details."""

from dynamodb_curated_library.core.config.constants import Constants
from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.core.config.job_parameters import JobParameters


def log_configuration(constants: Constants, params: JobParameters, job_config: JobConfig) -> None:
    """
    Log configuration details in an organized format.

    Args:
        constants: Constants configuration
        params: Job parameters
        job_config: Job configuration
    """
    print('=== CONSTANTS ===')
    print(constants)

    print('\n=== PARAMETERS ===')
    print(params)

    print('\n=== JOB CONFIG ===')
    print(job_config)
