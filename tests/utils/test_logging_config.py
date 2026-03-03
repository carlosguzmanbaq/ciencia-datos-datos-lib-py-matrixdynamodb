"""Tests for logging configuration utilities."""

import logging
from unittest.mock import patch, MagicMock, call

import pytest

from dynamodb_curated_library.utils.logging_config import configure_logging


def test_configure_logging_default_level():
    """Test configure_logging with default INFO level."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging()
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.INFO


def test_configure_logging_debug_level():
    """Test configure_logging with DEBUG level."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging(log_level="DEBUG")
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.DEBUG


def test_configure_logging_warning_level():
    """Test configure_logging with WARNING level."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging(log_level="WARNING")
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.WARNING


def test_configure_logging_error_level():
    """Test configure_logging with ERROR level."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging(log_level="ERROR")
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.ERROR


def test_configure_logging_critical_level():
    """Test configure_logging with CRITICAL level."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging(log_level="CRITICAL")
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.CRITICAL


def test_configure_logging_case_insensitive():
    """Test configure_logging handles lowercase level names."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging(log_level="debug")
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.DEBUG


def test_configure_logging_invalid_level_defaults_to_info():
    """Test configure_logging defaults to INFO for invalid level."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging(log_level="INVALID")
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.INFO


def test_configure_logging_silences_py4j_loggers():
    """Test configure_logging sets py4j loggers to WARNING."""
    with patch('logging.basicConfig'), \
         patch('logging.getLogger') as mock_get_logger:
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        configure_logging()
        
        # Verify getLogger was called for each py4j logger
        expected_calls = [
            call('py4j'),
            call('py4j.java_gateway'),
            call('py4j.clientserver')
        ]
        mock_get_logger.assert_has_calls(expected_calls, any_order=True)
        
        # Verify setLevel was called 3 times with WARNING
        assert mock_logger.setLevel.call_count == 3
        for call_args in mock_logger.setLevel.call_args_list:
            assert call_args[0][0] == logging.WARNING


def test_configure_logging_format():
    """Test configure_logging sets correct log format."""
    with patch('logging.basicConfig') as mock_basic_config:
        configure_logging()
        
        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['format'] == '%(levelname)s - %(name)s - %(message)s'
        assert call_kwargs['level'] == logging.INFO


def test_configure_logging_py4j_independent_of_root_level():
    """Test py4j loggers are WARNING regardless of root level."""
    with patch('logging.basicConfig') as mock_basic_config, \
         patch('logging.getLogger') as mock_get_logger:
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        configure_logging(log_level="DEBUG")
        
        # Root should be configured with DEBUG
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == logging.DEBUG
        
        # But py4j loggers should still be set to WARNING
        for call_args in mock_logger.setLevel.call_args_list:
            assert call_args[0][0] == logging.WARNING
