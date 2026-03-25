"""
TP3 — Shared pytest fixtures
=============================
Fixtures are fake DataFrames that mimic Bronze data.
They are automatically injected into tests by pytest when a test parameter
has the same name as a fixture function.

Example:
    # This fixture is defined here:
    @pytest.fixture
    def sample_products(): ...

    # Any test with "sample_products" as a parameter receives it automatically:
    def test_something(self, sample_products):
        # sample_products is the DataFrame returned by the fixture above
"""

import pytest
import pandas as pd


@pytest.fixture
def sample_products():
    """Fake products DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "product_id": [1, 2, 3],
        "display_name": ["Nike Air Max", "Adidas Ultraboost", "Jordan 1"],
        "brand": ["Nike", "Adidas", "Jordan"],
        "category": ["sneakers", "sneakers", "sneakers"],
        "price_usd": [149.99, 179.99, -10.00],  # one invalid price for testing
        "tags": ["running|casual", "running|boost", "retro|hype"],
        "is_active": [1, 1, 0],
        "is_hype_product": [0, 0, 1],
        "_internal_cost_usd": [50.0, 60.0, 70.0],
        "_supplier_id": ["SUP001", "SUP002", "SUP003"],
    })


@pytest.fixture
def sample_users():
    """Fake users DataFrame mimicking Bronze data."""
    # TODO: Create a DataFrame with these columns:
    #   - user_id: [1, 2]
    #   - email: include one with spaces and uppercase to test normalization
    #            e.g. [" Alice@Example.COM ", "bob@test.com"]
    #   - first_name, last_name: any values
    #   - loyalty_tier: include one None to test fillna
    #   - _hashed_password, _last_ip, _device_fingerprint: internal columns (should be removed)
    #
    # Hint: follow the same pattern as sample_products above

    #pass  # ← Replace with pd.DataFrame({...})
    """Fake users DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "user_id": [1, 2],
        "email": [" Alice@Example.COM ", "bob@test.com"],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Martin", "Smith"],
        "loyalty_tier": ["gold", None],
        "_hashed_password": ["abc123", "def456"],
        "_last_ip": ["1.2.3.4", "5.6.7.8"],
        "_device_fingerprint": ["fp1", "fp2"],
    })


@pytest.fixture
def sample_orders():
    """Fake orders DataFrame mimicking Bronze data."""
    # TODO: Create a DataFrame with these columns:
    #   - order_id: [1, 2, 3]
    #   - user_id: [1, 2, 1]
    #   - order_date: string dates like ["2026-02-10", "2026-02-11", "2026-02-12"]
    #   - status: include one invalid status like "invalid_status" to test filtering
    #   - total_usd: any positive values
    #   - coupon_code: include one None to test fillna
    #   - _stripe_charge_id, _fraud_score: internal columns (should be removed)
    #
    # Hint: follow the same pattern as sample_products above

    #pass  # ← Replace with pd.DataFrame({...})
    """Fake orders DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 1],
        "order_date": ["2026-02-10", "2026-02-11", "2026-02-12"],
        "status": ["delivered", "shipped", "invalid_status"],
        "total_usd": [149.99, 179.99, 50.0],
        "coupon_code": ["SAVE10", None, None],
        "_stripe_charge_id": ["ch_1", "ch_2", "ch_3"],
        "_fraud_score": [0.1, 0.2, 0.9],
    })

## rajout pour poouvoir faire + de coverage sur transform.py ( avec sample_order_line_items)
@pytest.fixture
def sample_order_line_items():
    return pd.DataFrame({
        "order_id": [1, 1, 2],
        "product_id": [101, 102, 103],
        "quantity": [2, 0, 1],
        "unit_price_usd": [50.0, 30.0, 20.0],
        "line_total_usd": [100.0, 0.0, 25.0],
        "_warehouse_id": ["W1", "W1", "W2"],
    })