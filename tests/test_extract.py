"""
TP3 — Unit tests for src/extract.py
=====================================

These tests verify that extraction functions correctly read from S3
and load into Bronze, without needing real AWS or database connections.

We mock:
  - _get_s3_client → so we don't need real AWS credentials
  - _load_to_bronze → so we don't need a real database
  - _read_csv_from_s3 / _read_jsonl_from_s3 / _read_partitioned_parquet_from_s3
    → to inject fake data
"""

from io import BytesIO
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.extract import (
    _read_csv_from_s3,
    _read_jsonl_from_s3,
    _read_partitioned_parquet_from_s3,
    _load_to_bronze,
    extract_products,
    extract_users,
    extract_orders,
    extract_order_line_items,
    extract_reviews,
    extract_clickstream,
)


# =============================================================================
# Helper tests
# =============================================================================

class TestReadCsvFromS3:
    """Tests for _read_csv_from_s3()."""

    @patch("src.extract._get_s3_client")
    def test_reads_csv_from_s3(self, mock_get_s3_client):
        # Fake CSV content returned by S3
        csv_content = "id,name\n1,Alice\n2,Bob\n"

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": BytesIO(csv_content.encode("utf-8"))
        }
        mock_get_s3_client.return_value = mock_s3

        result = _read_csv_from_s3("raw/test/file.csv")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["id", "name"]

    @patch("src.extract._get_s3_client", side_effect=Exception("S3 failed"))
    def test_propagates_error(self, mock_get_s3_client):
        with pytest.raises(Exception, match="S3 failed"):
            _read_csv_from_s3("raw/test/file.csv")


class TestReadJsonlFromS3:
    """Tests for _read_jsonl_from_s3()."""

    @patch("src.extract._get_s3_client")
    def test_reads_jsonl_from_s3(self, mock_get_s3_client):
        # Fake JSONL content returned by S3
        jsonl_content = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": BytesIO(jsonl_content.encode("utf-8"))
        }
        mock_get_s3_client.return_value = mock_s3

        result = _read_jsonl_from_s3("raw/test/file.jsonl")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "id" in result.columns
        assert "name" in result.columns

    @patch("src.extract._get_s3_client", side_effect=Exception("S3 failed"))
    def test_propagates_error(self, mock_get_s3_client):
        with pytest.raises(Exception, match="S3 failed"):
            _read_jsonl_from_s3("raw/test/file.jsonl")


class TestReadPartitionedParquetFromS3:
    """Tests for _read_partitioned_parquet_from_s3()."""

    @patch("src.extract.pq.read_table")
    @patch("src.extract._get_s3_client")
    def test_reads_partitioned_parquet_from_s3(self, mock_get_s3_client, mock_read_table):
        # Fake paginator returning 2 parquet files
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "raw/clickstream/part-1.parquet"},
                    {"Key": "raw/clickstream/part-2.parquet"},
                    {"Key": "raw/clickstream/readme.txt"},
                ]
            }
        ]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_s3.get_object.return_value = {"Body": BytesIO(b"fake parquet bytes")}
        mock_get_s3_client.return_value = mock_s3

        # Fake pyarrow table -> pandas DataFrame
        mock_table = MagicMock()
        mock_table.to_pandas.side_effect = [
            pd.DataFrame({"event_id": [1, 2]}),
            pd.DataFrame({"event_id": [3]}),
        ]
        mock_read_table.return_value = mock_table

        result = _read_partitioned_parquet_from_s3("raw/clickstream/")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "event_id" in result.columns

    @patch("src.extract._get_s3_client", side_effect=Exception("S3 failed"))
    def test_propagates_error(self, mock_get_s3_client):
        with pytest.raises(Exception, match="S3 failed"):
            _read_partitioned_parquet_from_s3("raw/clickstream/")


class TestLoadToBronze:
    """Tests for _load_to_bronze()."""

    @patch("src.extract.get_engine")
    def test_calls_to_sql(self, mock_get_engine):
        df = pd.DataFrame({"id": [1, 2]})

        with patch.object(df, "to_sql") as mock_to_sql:
            _load_to_bronze(df, "test_table")

            mock_to_sql.assert_called_once()
            _, kwargs = mock_to_sql.call_args
            assert kwargs["name"] == "test_table"
            assert kwargs["schema"] is not None
            assert kwargs["if_exists"] == "replace"
            assert kwargs["index"] is False

    @patch("src.extract.get_engine", side_effect=Exception("DB failed"))
    def test_propagates_error(self, mock_get_engine):
        df = pd.DataFrame({"id": [1]})

        with pytest.raises(Exception, match="DB failed"):
            _load_to_bronze(df, "test_table")
        

class TestExtractProducts:
    """Tests for extract_products()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_products):
        # Fake data returned by S3
        mock_read_csv.return_value = sample_products

        result = extract_products()

        # Check returned rows and Bronze loading
        assert len(result) == 3
        mock_load.assert_called_once_with(sample_products, "products")

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_products):
        mock_read_csv.return_value = sample_products

        result = extract_products()

        assert isinstance(result, pd.DataFrame)

    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 failed"))
    def test_propagates_error(self, mock_read_csv):
        with pytest.raises(Exception, match="S3 failed"):
            extract_products()


class TestExtractUsers:
    """Tests for extract_users()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_users):
        mock_read_csv.return_value = sample_users

        result = extract_users()

        assert len(result) == 2
        mock_load.assert_called_once_with(sample_users, "users")

    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 failed"))
    def test_propagates_error(self, mock_read_csv):
        with pytest.raises(Exception, match="S3 failed"):
            extract_users()


class TestExtractOrders:
    """Tests for extract_orders()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_orders):
        mock_read_csv.return_value = sample_orders

        result = extract_orders()

        assert len(result) == 3
        mock_load.assert_called_once_with(sample_orders, "orders")

    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 failed"))
    def test_propagates_error(self, mock_read_csv):
        with pytest.raises(Exception, match="S3 failed"):
            extract_orders()


class TestExtractOrderLineItems:
    """Tests for extract_order_line_items()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_order_line_items):
        mock_read_csv.return_value = sample_order_line_items

        result = extract_order_line_items()

        assert len(result) == 3
        mock_load.assert_called_once_with(sample_order_line_items, "order_line_items")

    @patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 failed"))
    def test_propagates_error(self, mock_read_csv):
        with pytest.raises(Exception, match="S3 failed"):
            extract_order_line_items()


class TestExtractReviews:
    """Tests for extract_reviews()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extracts_and_loads(self, mock_read_jsonl, mock_load):
        sample_reviews = pd.DataFrame(
            {
                "review_id": [1, 2],
                "product_id": ["P1", "P2"],
                "rating": [5, 4],
            }
        )
        mock_read_jsonl.return_value = sample_reviews

        result = extract_reviews()

        assert len(result) == 2
        mock_load.assert_called_once_with(sample_reviews, "reviews")

    @patch("src.extract._read_jsonl_from_s3", side_effect=Exception("JSONL failed"))
    def test_propagates_error(self, mock_read_jsonl):
        with pytest.raises(Exception, match="JSONL failed"):
            extract_reviews()


class TestExtractClickstream:
    """Tests for extract_clickstream()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_extracts_and_loads(self, mock_read_parquet, mock_load):
        sample_clickstream = pd.DataFrame(
            {
                "event_id": [1, 2, 3],
                "user_id": [10, 11, 12],
                "event_type": ["view", "click", "cart"],
            }
        )
        mock_read_parquet.return_value = sample_clickstream

        result = extract_clickstream()

        assert len(result) == 3
        mock_load.assert_called_once_with(sample_clickstream, "clickstream")

    @patch("src.extract._read_partitioned_parquet_from_s3", side_effect=Exception("Parquet failed"))
    def test_propagates_error(self, mock_read_parquet):
        with pytest.raises(Exception, match="Parquet failed"):
            extract_clickstream()

