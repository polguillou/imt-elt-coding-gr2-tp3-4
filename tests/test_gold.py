"""
TP3 — Unit tests for src/gold.py
================================

These tests verify that Gold layer functions correctly build analytical
tables from the Silver layer, without needing a real database connection.

We mock:
  - get_engine → so we don't need a real database engine
  - pd.read_sql → to inject fake query results
  - _create_gold_table → so we don't actually write tables to the database
  - create_* functions → to test the orchestration of create_gold_layer()
"""

import pandas as pd
import pytest
from unittest.mock import patch

from src.gold import (
    create_daily_revenue,
    create_product_performance,
    create_customer_ltv,
    create_gold_layer,
)


class TestCreateDailyRevenue:
    """Tests for create_daily_revenue()."""

    @patch("src.gold._create_gold_table")
    @patch("src.gold.get_engine")
    @patch("src.gold.pd.read_sql")
    def test_creates_daily_revenue_table(self, mock_read_sql, mock_get_engine, mock_create_table):
        # Fake SQL result returned by pd.read_sql()
        sample_df = pd.DataFrame(
            {
                "order_date": ["2025-01-01"],
                "total_orders": [2],
                "total_revenue": [150.0],
                "avg_order_value": [75.0],
                "total_items": [3],
            }
        )
        mock_read_sql.return_value = sample_df

        create_daily_revenue()

        # Check that the SQL query was executed
        mock_read_sql.assert_called_once()

        # Check that the Gold table creation was called with the right name
        mock_create_table.assert_called_once_with(sample_df, "daily_revenue")


class TestCreateProductPerformance:
    """Tests for create_product_performance()."""

    @patch("src.gold._create_gold_table")
    @patch("src.gold.get_engine")
    @patch("src.gold.pd.read_sql")
    def test_creates_product_performance_table(self, mock_read_sql, mock_get_engine, mock_create_table):
        # Fake SQL result returned by pd.read_sql()
        sample_df = pd.DataFrame(
            {
                "product_id": ["P001"],
                "product_name": ["Sneaker X"],
                "brand": ["Nike"],
                "category": ["Shoes"],
                "total_quantity_sold": [10],
                "total_revenue": [1000.0],
                "num_orders": [5],
                "avg_unit_price": [100.0],
            }
        )
        mock_read_sql.return_value = sample_df

        create_product_performance()

        # Check that the SQL query was executed
        mock_read_sql.assert_called_once()

        # Check that the Gold table creation was called with the right name
        mock_create_table.assert_called_once_with(sample_df, "product_performance")


class TestCreateCustomerLTV:
    """Tests for create_customer_ltv()."""

    @patch("src.gold._create_gold_table")
    @patch("src.gold.get_engine")
    @patch("src.gold.pd.read_sql")
    def test_creates_customer_ltv_table(self, mock_read_sql, mock_get_engine, mock_create_table):
        # Fake SQL result returned by pd.read_sql()
        sample_df = pd.DataFrame(
            {
                "user_id": [1],
                "email": ["alice@example.com"],
                "first_name": ["Alice"],
                "last_name": ["Smith"],
                "loyalty_tier": ["gold"],
                "total_orders": [3],
                "total_spent": [250.0],
                "avg_order_value": [83.33],
                "first_order_date": ["2025-01-01"],
                "last_order_date": ["2025-01-10"],
                "days_as_customer": [9],
            }
        )
        mock_read_sql.return_value = sample_df

        create_customer_ltv()

        # Check that the SQL query was executed
        mock_read_sql.assert_called_once()

        # Check that the Gold table creation was called with the right name
        mock_create_table.assert_called_once_with(sample_df, "customer_ltv")


class TestCreateGoldLayer:
    """Tests for create_gold_layer()."""

    @patch("src.gold.create_customer_ltv")
    @patch("src.gold.create_product_performance")
    @patch("src.gold.create_daily_revenue")
    def test_calls_all_gold_functions(self, mock_daily, mock_product, mock_customer):
        create_gold_layer()

        # Check that all Gold creation functions are called once
        mock_daily.assert_called_once()
        mock_product.assert_called_once()
        mock_customer.assert_called_once()


class TestGoldErrorHandling:
    """Tests that exceptions are propagated correctly."""

    @patch("src.gold.pd.read_sql", side_effect=Exception("SQL failed"))
    def test_create_daily_revenue_propagates_error(self, mock_read_sql):
        with pytest.raises(Exception, match="SQL failed"):
            create_daily_revenue()

    @patch("src.gold.pd.read_sql", side_effect=Exception("SQL failed"))
    def test_create_product_performance_propagates_error(self, mock_read_sql):
        with pytest.raises(Exception, match="SQL failed"):
            create_product_performance()

    @patch("src.gold.pd.read_sql", side_effect=Exception("SQL failed"))
    def test_create_customer_ltv_propagates_error(self, mock_read_sql):
        with pytest.raises(Exception, match="SQL failed"):
            create_customer_ltv()

    @patch("src.gold.create_daily_revenue", side_effect=Exception("Gold failed"))
    def test_create_gold_layer_propagates_error(self, mock_daily):
        with pytest.raises(Exception, match="Gold failed"):
            create_gold_layer()