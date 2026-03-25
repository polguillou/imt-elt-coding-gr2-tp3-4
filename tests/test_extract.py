import pandas as pd
import pytest
from unittest.mock import patch

from src.extract import (
    extract_products,
    extract_users,
    extract_orders,
    extract_order_line_items,
    extract_reviews,
    extract_clickstream,
    extract_all,
)


class TestExtractProducts:
    """Tests for extract_products()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_products):
        mock_read_csv.return_value = sample_products

        result = extract_products()

        assert len(result) == len(sample_products)
        mock_load.assert_called_once_with(sample_products, "products")

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_products):
        mock_read_csv.return_value = sample_products

        result = extract_products()

        assert isinstance(result, pd.DataFrame)


class TestExtractUsers:
    """Tests for extract_users()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_users):
        mock_read_csv.return_value = sample_users

        result = extract_users()

        assert len(result) == len(sample_users)
        mock_load.assert_called_once_with(sample_users, "users")


class TestExtractOrders:
    """Tests for extract_orders()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_orders):
        mock_read_csv.return_value = sample_orders

        result = extract_orders()

        assert len(result) == len(sample_orders)
        mock_load.assert_called_once_with(sample_orders, "orders")


class TestExtractOrderLineItems:
    """Tests for extract_order_line_items()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_order_line_items):
        mock_read_csv.return_value = sample_order_line_items

        result = extract_order_line_items()

        assert len(result) == len(sample_order_line_items)
        mock_load.assert_called_once_with(sample_order_line_items, "order_line_items")


class TestExtractReviews:
    """Tests for extract_reviews()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extracts_and_loads(self, mock_read_jsonl, mock_load, sample_reviews):
        mock_read_jsonl.return_value = sample_reviews

        result = extract_reviews()

        assert len(result) == len(sample_reviews)
        mock_load.assert_called_once_with(sample_reviews, "reviews")


class TestExtractClickstream:
    """Tests for extract_clickstream()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_extracts_and_loads(self, mock_read_parquet, mock_load, sample_clickstream):
        mock_read_parquet.return_value = sample_clickstream

        result = extract_clickstream()

        assert len(result) == len(sample_clickstream)
        mock_load.assert_called_once_with(sample_clickstream, "clickstream")


class TestExtractErrorHandling:
    """Tests for error propagation in extract functions."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 read failed"))
    def test_extract_products_propagates_error(self, mock_read_csv, mock_load):
        with pytest.raises(Exception, match="S3 read failed"):
            extract_products()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 read failed"))
    def test_extract_users_propagates_error(self, mock_read_csv, mock_load):
        with pytest.raises(Exception, match="S3 read failed"):
            extract_users()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 read failed"))
    def test_extract_orders_propagates_error(self, mock_read_csv, mock_load):
        with pytest.raises(Exception, match="S3 read failed"):
            extract_orders()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 read failed"))
    def test_extract_order_line_items_propagates_error(self, mock_read_csv, mock_load):
        with pytest.raises(Exception, match="S3 read failed"):
            extract_order_line_items()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3", side_effect=Exception("S3 read failed"))
    def test_extract_reviews_propagates_error(self, mock_read_jsonl, mock_load):
        with pytest.raises(Exception, match="S3 read failed"):
            extract_reviews()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3", side_effect=Exception("S3 read failed"))
    def test_extract_clickstream_propagates_error(self, mock_read_parquet, mock_load):
        with pytest.raises(Exception, match="S3 read failed"):
            extract_clickstream()


class TestExtractAll:
    """Tests for extract_all()."""

    @patch("src.extract.extract_clickstream")
    @patch("src.extract.extract_reviews")
    @patch("src.extract.extract_order_line_items")
    @patch("src.extract.extract_orders")
    @patch("src.extract.extract_users")
    @patch("src.extract.extract_products")
    def test_extract_all_success(
        self,
        mock_products,
        mock_users,
        mock_orders,
        mock_order_line_items,
        mock_reviews,
        mock_clickstream,
    ):
        mock_products.return_value = pd.DataFrame({"id": [1]})
        mock_users.return_value = pd.DataFrame({"id": [1]})
        mock_orders.return_value = pd.DataFrame({"id": [1]})
        mock_order_line_items.return_value = pd.DataFrame({"id": [1]})
        mock_reviews.return_value = pd.DataFrame({"id": [1]})
        mock_clickstream.return_value = pd.DataFrame({"id": [1]})

        result = extract_all()

        assert "products" in result
        assert "users" in result
        assert "orders" in result
        assert "order_line_items" in result
        assert "reviews" in result
        assert "clickstream" in result
        assert len(result) == 6