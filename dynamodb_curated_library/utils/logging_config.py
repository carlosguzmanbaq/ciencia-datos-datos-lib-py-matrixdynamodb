"""Logging configuration utilities."""

import logging


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Configure basic logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(levelname)s - %(name)s - %(message)s'
    )

    # Silence noisy third-party loggers
    logging.getLogger('py4j').setLevel(logging.WARNING)
    logging.getLogger('py4j.java_gateway').setLevel(logging.WARNING)
    logging.getLogger('py4j.clientserver').setLevel(logging.WARNING)
