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
    extract_all,
)


def test_read_csv_from_s3():
    # Fake CSV content returned by S3
    csv_content = "id,name\n1,Alice\n2,Bob\n"
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": BytesIO(csv_content.encode("utf-8"))
    }

    with patch("src.extract._get_s3_client", return_value=mock_s3):
        df = _read_csv_from_s3("raw/test/file.csv")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]


def test_read_jsonl_from_s3():
    # Fake JSONL content returned by S3
    jsonl_content = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": BytesIO(jsonl_content.encode("utf-8"))
    }

    with patch("src.extract._get_s3_client", return_value=mock_s3):
        df = _read_jsonl_from_s3("raw/test/file.jsonl")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "id" in df.columns
    assert "name" in df.columns


def test_read_partitioned_parquet_from_s3():
    # Fake paginator returning parquet files
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw/clickstream/part-1.parquet"},
                {"Key": "raw/clickstream/part-2.parquet"},
            ]
        }
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.get_object.return_value = {"Body": BytesIO(b"fake parquet bytes")}

    mock_table = MagicMock()
    mock_table.to_pandas.side_effect = [
        pd.DataFrame({"event_id": [1, 2]}),
        pd.DataFrame({"event_id": [3]}),
    ]

    with patch("src.extract._get_s3_client", return_value=mock_s3):
        with patch("src.extract.pq.read_table", return_value=mock_table):
            df = _read_partitioned_parquet_from_s3("raw/clickstream/")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "event_id" in df.columns


def test_load_to_bronze():
    df = pd.DataFrame({"id": [1, 2]})

    with patch("src.extract.get_engine"):
        with patch.object(df, "to_sql") as mock_to_sql:
            _load_to_bronze(df, "test_table")

    mock_to_sql.assert_called_once()


def test_extract_products(sample_products):
    with patch("src.extract._read_csv_from_s3", return_value=sample_products):
        with patch("src.extract._load_to_bronze") as mock_load:
            result = extract_products()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_products)
    mock_load.assert_called_once_with(sample_products, "products")


def test_extract_users(sample_users):
    with patch("src.extract._read_csv_from_s3", return_value=sample_users):
        with patch("src.extract._load_to_bronze") as mock_load:
            result = extract_users()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_users)
    mock_load.assert_called_once_with(sample_users, "users")


def test_extract_orders(sample_orders):
    with patch("src.extract._read_csv_from_s3", return_value=sample_orders):
        with patch("src.extract._load_to_bronze") as mock_load:
            result = extract_orders()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_orders)
    mock_load.assert_called_once_with(sample_orders, "orders")


def test_extract_order_line_items(sample_order_line_items):
    with patch("src.extract._read_csv_from_s3", return_value=sample_order_line_items):
        with patch("src.extract._load_to_bronze") as mock_load:
            result = extract_order_line_items()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_order_line_items)
    mock_load.assert_called_once_with(sample_order_line_items, "order_line_items")


def test_extract_reviews():
    sample_reviews = pd.DataFrame(
        {
            "review_id": [1, 2],
            "product_id": ["P1", "P2"],
            "rating": [5, 4],
        }
    )

    with patch("src.extract._read_jsonl_from_s3", return_value=sample_reviews):
        with patch("src.extract._load_to_bronze") as mock_load:
            result = extract_reviews()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    mock_load.assert_called_once_with(sample_reviews, "reviews")


def test_extract_clickstream():
    sample_clickstream = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "user_id": [10, 11, 12],
            "event_type": ["view", "click", "cart"],
        }
    )

    with patch("src.extract._read_partitioned_parquet_from_s3", return_value=sample_clickstream):
        with patch("src.extract._load_to_bronze") as mock_load:
            result = extract_clickstream()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    mock_load.assert_called_once_with(sample_clickstream, "clickstream")


def test_extract_all():
    with patch("src.extract.extract_products", return_value=pd.DataFrame({"id": [1]})):
        with patch("src.extract.extract_users", return_value=pd.DataFrame({"id": [1]})):
            with patch("src.extract.extract_orders", return_value=pd.DataFrame({"id": [1]})):
                with patch("src.extract.extract_order_line_items", return_value=pd.DataFrame({"id": [1]})):
                    with patch("src.extract.extract_reviews", return_value=pd.DataFrame({"id": [1]})):
                        with patch("src.extract.extract_clickstream", return_value=pd.DataFrame({"id": [1]})):
                            result = extract_all()

    assert isinstance(result, dict)
    assert "products" in result
    assert "users" in result
    assert "orders" in result
    assert "order_line_items" in result
    assert "reviews" in result
    assert "clickstream" in result
    assert len(result) == 6


def test_extract_products_error():
    with patch("src.extract._read_csv_from_s3", side_effect=Exception("S3 failed")):
        with pytest.raises(Exception, match="S3 failed"):
            extract_products()


def test_extract_reviews_error():
    with patch("src.extract._read_jsonl_from_s3", side_effect=Exception("JSONL failed")):
        with pytest.raises(Exception, match="JSONL failed"):
            extract_reviews()


def test_extract_clickstream_error():
    with patch("src.extract._read_partitioned_parquet_from_s3", side_effect=Exception("Parquet failed")):
        with pytest.raises(Exception, match="Parquet failed"):
            extract_clickstream()