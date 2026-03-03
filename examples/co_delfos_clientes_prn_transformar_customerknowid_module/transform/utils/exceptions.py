"""Custom exceptions for DynamoDB transformations."""


class TransformationException(Exception):
    """Exception raised when DynamoDB transformation fails."""

    def __init__(self, message: str, table_name: str = None): # pragma: no cover
        self.table_name = table_name
        super().__init__(message)
