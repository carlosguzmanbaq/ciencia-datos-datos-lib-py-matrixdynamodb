"""
Test for the specific notifications conflict case from user's schema.

This test validates that the flatten function correctly handles the exact
structure provided by the user where notifications has both L and NULL types.
"""

from pyspark.sql import  types as T
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import flatten_dynamodb_struct


def test_user_notifications_schema_conflict(spark):
    """
    Test the exact schema structure provided by the user.

    The notifications field has both:
    - L: array with complex nested structure
    - NULL: boolean value

    Should prioritize the L (array) type and ignore NULL.
    """
    # Exact schema from user's structure
    schema = T.StructType([
        T.StructField("Item", T.StructType([
            T.StructField("creationDate", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("customerId", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("email", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("firstName", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("notificationDate", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("notifications", T.StructType([
                T.StructField("L", T.ArrayType(T.StructType([
                    T.StructField("M", T.StructType([
                        T.StructField("messageId", T.StructType([
                            T.StructField("NULL", T.BooleanType(), True),
                            T.StructField("S", T.StringType(), True)
                        ]), True),
                        T.StructField("notificationDate", T.StructType([
                            T.StructField("S", T.StringType(), True)
                        ]), True),
                        T.StructField("notified", T.StructType([
                            T.StructField("BOOL", T.BooleanType(), True)
                        ]), True),
                        T.StructField("notifiedDate", T.StructType([
                            T.StructField("NULL", T.BooleanType(), True),
                            T.StructField("S", T.StringType(), True)
                        ]), True),
                        T.StructField("type", T.StructType([
                            T.StructField("S", T.StringType(), True)
                        ]), True)
                    ]), True)
                ])), True),
                T.StructField("NULL", T.BooleanType(), True)
            ]), True),
            T.StructField("notified", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("owner", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("phoneNumber", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("products", T.StructType([
                T.StructField("M", T.StructType([
                    T.StructField("credits", T.StructType([
                        T.StructField("BOOL", T.BooleanType(), True),
                        T.StructField("S", T.StringType(), True)
                    ]), True),
                    T.StructField("paypal", T.StructType([
                        T.StructField("BOOL", T.BooleanType(), True),
                        T.StructField("S", T.StringType(), True)
                    ]), True),
                    T.StructField("remittances", T.StructType([
                        T.StructField("BOOL", T.BooleanType(), True),
                        T.StructField("S", T.StringType(), True)
                    ]), True),
                    T.StructField("savingsAccount", T.StructType([
                        T.StructField("BOOL", T.BooleanType(), True),
                        T.StructField("S", T.StringType(), True)
                    ]), True)
                ]), True)
            ]), True),
            T.StructField("risk", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True),
            T.StructField("updateDate", T.StructType([
                T.StructField("S", T.StringType(), True)
            ]), True)
        ]), True)
    ])

    # Test data with both notifications.L and notifications.NULL
    data = [
        {
            "Item": {
                "creationDate": {"S": "2024-01-15"},
                "customerId": {"S": "CUST123"},
                "email": {"S": "user@example.com"},
                "firstName": {"S": "John"},
                "notificationDate": {"S": "2024-01-15"},
                "notifications": {
                    "L": [
                        {
                            "M": {
                                "messageId": {"S": "msg001"},
                                "notificationDate": {"S": "2024-01-15"},
                                "notified": {"BOOL": True},
                                "notifiedDate": {"S": "2024-01-15"},
                                "type": {"S": "email"}
                            }
                        },
                        {
                            "M": {
                                "messageId": {"NULL": True},
                                "notificationDate": {"S": "2024-01-16"},
                                "notified": {"BOOL": False},
                                "notifiedDate": {"NULL": True},
                                "type": {"S": "sms"}
                            }
                        }
                    ],
                    "NULL": True  # This should be ignored due to lower priority
                },
                "notified": {"S": "true"},
                "owner": {"S": "system"},
                "phoneNumber": {"S": "+1234567890"},
                "products": {
                    "M": {
                        "credits": {"S": "enabled"},
                        "paypal": {"BOOL": True},
                        "remittances": {"S": "active"},
                        "savingsAccount": {"BOOL": False}
                    }
                },
                "risk": {"S": "low"},
                "updateDate": {"S": "2024-01-15"}
            }
        }
    ]

    df = spark.createDataFrame(data, schema)

    result = flatten_dynamodb_struct(
        df=df,
        parent_col="Item",
        max_depth=3,
        columns_as_json=None
    )

    # Verify structure
    columns = result.columns

    # Basic fields should be flattened
    assert "creation_date" in columns
    assert "customer_id" in columns
    assert "email" in columns
    assert "first_name" in columns
    assert "notification_date" in columns
    assert "notified" in columns
    assert "owner" in columns
    assert "phone_number" in columns
    assert "risk" in columns
    assert "update_date" in columns

    # notifications should be array (L priority over NULL)
    assert "notifications" in columns
    notifications_field = next(f for f in result.schema.fields if f.name == "notifications")
    assert isinstance(notifications_field.dataType, T.ArrayType)

    # products should be flattened (M type)
    assert "products_credits" in columns
    assert "products_paypal" in columns
    assert "products_remittances" in columns
    assert "products_savings_account" in columns

    # Verify data content
    row = result.collect()[0]
    assert row["customer_id"] == "CUST123"
    assert row["email"] == "user@example.com"
    assert row["first_name"] == "John"

    # Verify notifications array structure
    notifications = row["notifications"]
    assert len(notifications) == 2
    assert notifications[0]["message_id"] == "msg001"
    assert notifications[0]["type"] == "email"
    assert notifications[1]["message_id"] is None  # NULL value
    assert notifications[1]["type"] == "sms"

    # Verify products flattening
    assert row["products_credits"] == "enabled"
    assert row["products_paypal"] == "true"  # BOOL converted to string
    assert row["products_remittances"] == "active"
    assert row["products_savings_account"] == "false"  # BOOL converted to string


def test_products_field_conflicts_uses_coalesce(spark):
    """
    Test that products subfields with conflicting value types use coalesce
    to preserve all non-null values.

    This addresses the real-world scenario where BOOL values were being lost.
    """
    schema = T.StructType([
        T.StructField("Item", T.StructType([
            T.StructField("products", T.StructType([
                T.StructField("M", T.StructType([
                    T.StructField("credits", T.StructType([
                        T.StructField("BOOL", T.BooleanType(), True),
                        T.StructField("S", T.StringType(), True)
                    ]), True)
                ]), True)
            ]), True)
        ]), True)
    ])

    data = [
        # Case 1: Only BOOL has value (should preserve BOOL)
        {
            "Item": {
                "products": {
                    "M": {
                        "credits": {
                            "BOOL": True,
                            "S": None
                        }
                    }
                }
            }
        },
        # Case 2: Only S has value (should preserve S)
        {
            "Item": {
                "products": {
                    "M": {
                        "credits": {
                            "BOOL": None,
                            "S": "premium_enabled"
                        }
                    }
                }
            }
        },
        # Case 3: Both have values (coalesce should pick S first)
        {
            "Item": {
                "products": {
                    "M": {
                        "credits": {
                            "BOOL": False,
                            "S": "override_value"
                        }
                    }
                }
            }
        }
    ]

    df = spark.createDataFrame(data, schema)

    result = flatten_dynamodb_struct(
        df=df,
        parent_col="Item",
        max_depth=3,
        columns_as_json=None
    )

    rows = result.collect()
    # Should preserve all values using coalesce
    assert rows[0]["products_credits"] == "true"           # BOOL preserved
    assert rows[1]["products_credits"] == "premium_enabled" # S preserved
    assert rows[2]["products_credits"] == "override_value"  # S takes precedence in coalesce
