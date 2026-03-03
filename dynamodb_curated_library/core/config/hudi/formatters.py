"""Utility functions for formatting configuration objects."""

from typing import Dict, Any


def format_hudi_options(hudi_options: Dict[str, Any]) -> str: # pragma: no cover
    """
    Format Hudi options dictionary for display.

    Args:
        hudi_options: Dictionary containing Hudi configuration options

    Returns:
        Formatted string with each option on a new line
    """
    if isinstance(hudi_options, dict):
        lines = [f"  {key}: {value}" for key, value in hudi_options.items()]
        return "\n".join(lines)
    return f"  {hudi_options}"
