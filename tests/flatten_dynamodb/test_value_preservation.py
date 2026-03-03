"""
Test for value preservation in conflicting DynamoDB types.

This test validates that when multiple value types exist (S, N, BOOL, NULL),
all non-null values are preserved using coalesce instead of losing data through prioritization.
"""

from pyspark.sql import types as T
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import flatten_dynamodb_struct


def test_value_preservation_with_coalesce(spark):
    """
    Test that coalesce preserves all non-null values from different DynamoDB types.

    This addresses the issue where prioritization was losing valuable data.
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

    # Test data simulating the user's real scenario
    data = [
        # Case 1: BOOL has value, S is null (should get BOOL value)
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
        # Case 2: BOOL has value, S is null (should get BOOL value)
        {
            "Item": {
                "products": {
                    "M": {
                        "credits": {
                            "BOOL": False,
                            "S": None
                        }
                    }
                }
            }
        },
        # Case 3: S has value, BOOL is null (should get S value)
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
        # Case 4: Both have values (should get S value due to coalesce order)
        {
            "Item": {
                "products": {
                    "M": {
                        "credits": {
                            "BOOL": True,
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

    # Verify all values are preserved
    assert rows[0]["products_credits"] == "true"  # BOOL True converted to string
    assert rows[1]["products_credits"] == "false"  # BOOL False converted to string
    assert rows[2]["products_credits"] == "premium_enabled"  # S value
    assert rows[3]["products_credits"] == "override_value"  # S takes precedence in coalesce


def test_real_world_credits_scenario(spark):
    """
    Test the exact scenario from user's data:
    - 87,409 records with BOOL=false
    - 5,111 records with BOOL=true
    - 1 record with S="1"
    - 92,520 records with S=null

    Should preserve all BOOL values and the single S value.
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

    # Simulate the user's data distribution
    data = []

    # 87,409 records with BOOL=false, S=null (simulate with 3 records)
    for _ in range(3):
        data.append({
            "Item": {
                "products": {
                    "M": {
                        "credits": {
                            "BOOL": False,
                            "S": None
                        }
                    }
                }
            }
        })

    # 5,111 records with BOOL=true, S=null (simulate with 2 records)
    for _ in range(2):
        data.append({
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
        })

    # 1 record with S="1", BOOL=null
    data.append({
        "Item": {
            "products": {
                "M": {
                    "credits": {
                        "BOOL": None,
                        "S": "1"
                    }
                }
            }
        }
    })

    df = spark.createDataFrame(data, schema)

    result = flatten_dynamodb_struct(
        df=df,
        parent_col="Item",
        max_depth=3,
        columns_as_json=None
    )

    # Verify value distribution
    value_counts = result.groupBy("products_credits").count().collect()
    value_dict = {row["products_credits"]: row["count"] for row in value_counts}

    # Should have preserved all values
    assert value_dict.get("false", 0) == 3  # BOOL false values preserved
    assert value_dict.get("true", 0) == 2   # BOOL true values preserved
    assert value_dict.get("1", 0) == 1      # S value preserved
    assert value_dict.get(None, 0) == 0     # No null values lost


def test_coalesce_order_s_n_bool(spark):
    """
    Test that coalesce follows the order S, N, BOOL to prioritize string values
    when multiple types have non-null values.
    """
    schema = T.StructType([
        T.StructField("Item", T.StructType([
            T.StructField("field", T.StructType([
                T.StructField("S", T.StringType(), True),
                T.StructField("N", T.StringType(), True),  # DynamoDB stores numbers as strings
                T.StructField("BOOL", T.BooleanType(), True)
            ]), True)
        ]), True)
    ])

    data = [
        # All types have values - should prioritize S
        {
            "Item": {
                "field": {
                    "S": "string_value",
                    "N": "123",
                    "BOOL": True
                }
            }
        },
        # Only N and BOOL - should prioritize N
        {
            "Item": {
                "field": {
                    "S": None,
                    "N": "456",
                    "BOOL": False
                }
            }
        },
        # Only BOOL - should get BOOL
        {
            "Item": {
                "field": {
                    "S": None,
                    "N": None,
                    "BOOL": True
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

    assert rows[0]["field"] == "string_value"  # S takes precedence
    assert rows[1]["field"] == "456"           # N takes precedence over BOOL
    assert rows[2]["field"] == "true"          # BOOL when others are null
