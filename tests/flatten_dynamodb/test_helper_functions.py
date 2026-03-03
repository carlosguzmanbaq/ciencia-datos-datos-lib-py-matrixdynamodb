"""Tests for helper functions (to_snake_case, calculate_depth)."""

from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import (
    to_snake_case,
    calculate_depth
)


class TestHelperFunctions:
    """Test helper functions."""

    def test_to_snake_case(self):
        """Test camelCase to snake_case conversion."""
        assert to_snake_case("orderId") == "order_id"
        assert to_snake_case("remittanceId") == "remittance_id"
        assert to_snake_case("orderSteps") == "order_steps"
        assert to_snake_case("simpleString") == "simple_string"
        assert to_snake_case("alreadysnake") == "alreadysnake"

    def test_calculate_depth(self):
        """Test depth calculation excluding DynamoDB markers."""
        assert calculate_depth("orderId") == 1
        assert calculate_depth("orderId.S") == 1
        assert calculate_depth("state.M.orderId") == 2
        assert calculate_depth("state.M.orderId.S") == 2
        assert calculate_depth("state.M.orderSteps.M.items") == 3
        assert calculate_depth("state.M.orderSteps.M.items.L") == 3
