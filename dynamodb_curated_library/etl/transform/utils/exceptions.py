"""Custom exceptions for flatten_dynamodb_struct module."""


class FlattenException(Exception):
    """Base exception for flatten operations."""

    def __init__(self, message: str, original_error: Exception = None, suggestions: list = None):
        """
        Initialize FlattenException.

        Args:
            message: Error message
            original_error: Original exception that caused this error
            suggestions: List of suggestions to fix the issue
        """
        self.message = message
        self.original_error = original_error
        self.suggestions = suggestions or []
        super().__init__(self.format_error())

    def format_error(self) -> str:
        """Format error message with suggestions."""
        lines = [
            "\n" + "=" * 80,
            "ERROR: Failed to flatten DynamoDB structure",
            "=" * 80,
            f"Details: {self.message}",
        ]

        if self.original_error:
            lines.append(f"Caused by: {str(self.original_error)}")

        if self.suggestions:
            lines.append("\nSUGGESTIONS:")
            for i, suggestion in enumerate(self.suggestions, 1):
                lines.append(f"  {i}. {suggestion}")

        lines.append("=" * 80 + "\n")
        return "\n".join(lines)


class FlattenStructureException(FlattenException):
    """Exception raised when structure cannot be flattened."""

    def __init__(self, original_error: Exception, max_depth: int, parent_col: str):
        """
        Initialize FlattenStructureException.

        Args:
            original_error: Original Spark exception
            max_depth: Current max_depth value
            parent_col: Parent column being processed
        """
        message = "The structure might be too complex or vary between records"

        suggestions = [
            f"Reduce max_depth parameter (current: {max_depth})",
            "Add problematic columns to 'columns_as_json' parameter",
            f"\nExample:\n"
            f"  flatten_dynamodb_struct(\n"
            f"      df,\n"
            f"      parent_col='{parent_col}',\n"
            f"      max_depth={max(1, max_depth - 1)},\n"
            f"      columns_as_json=['state.M.orderSteps', 'metadata.M.complex']\n"
            f"  )"
        ]

        super().__init__(message, original_error, suggestions)
