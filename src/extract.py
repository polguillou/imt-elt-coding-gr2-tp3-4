"""
SOLUTION — Extract (Bronze Layer)
Reads files from S3 data lake (CSV, JSONL, Parquet) → loads into Bronze schema.
"""

import os
from io import StringIO, BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text

from src.database import get_engine, BRONZE_SCHEMA

# TODO (TP3): Import your logger and create a module-level logger
from src.logger import get_logger
logger = get_logger(__name__)

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "kickz-empire-data")
S3_PREFIX = os.getenv("S3_PREFIX", "raw")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")


def _get_s3_client():
    """Create and return a boto3 S3 client."""
    try:
        client= boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        logger.info("S3 client successfully initialized!")
        return client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}", exc_info=True)
        raise
    finally:
        logger.debug("S3 client connection attempt finished.")


# ---------------------------------------------------------------------------
# Read helpers — one per format
# ---------------------------------------------------------------------------
def _read_csv_from_s3(s3_key: str) -> pd.DataFrame:
    """Read a CSV file from S3 into a pandas DataFrame."""
    # TODO: Download the CSV from S3 and return it as a DataFrame
    # Steps: get S3 client → get_object() → read & decode the body → pd.read_csv()
    # Remember: read_csv() expects a file-like object, not a raw string
    try:
        s3 = _get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        csv_content = response["Body"].read().decode("utf-8")
        logger.info("S3 successfully read!")
        return pd.read_csv(StringIO(csv_content))
    except Exception as e :
        logger.error(f"Failed to read from S3 client: {e}", exc_info=True)
        raise
    finally:
        logger.debug("S3 client reading attempt finished.")    


def _read_jsonl_from_s3(s3_key: str) -> pd.DataFrame:
    """Read a JSONL (newline-delimited JSON) file from S3 into a DataFrame."""
    # TODO: Download the JSONL from S3 and return it as a DataFrame
    # Very similar to _read_csv_from_s3(), but use pd.read_json() instead.
    # Key parameter: lines=True (tells pandas each line is a separate JSON object)
    try:
        s3 = _get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        jsonl_content = response["Body"].read().decode("utf-8")
        logger.info("JSONL from S3 successfully read!")
        return pd.read_json(StringIO(jsonl_content), lines=True)
    except Exception as e:
        logger.error(f"Failed to read JSONL from S3 client: {e}", exc_info=True)
        raise
    finally:
        logger.debug("JSONL S3 client reading attempt finished.")    




def _read_partitioned_parquet_from_s3(s3_prefix: str) -> pd.DataFrame:
    """Read a date-partitioned Parquet dataset from S3 into a DataFrame."""
    # TODO: List all Parquet files under s3_prefix and concatenate them
    # Strategy:
    #   1. Use s3.get_paginator("list_objects_v2") to list all objects under the prefix
    #   2. Filter keys that end with ".parquet"
    #   3. For each file: download with get_object(), read with pq.read_table()
    #      (Parquet is binary → use BytesIO, not StringIO)
    #   4. Collect all DataFrames in a list, then pd.concat() them
    try:
        s3 = _get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        dfs = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
                    table = pq.read_table(BytesIO(response["Body"].read()))
                    dfs.append(table.to_pandas())
        logger.info("Parquet from S3 successfully read!")
        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        logger.error(f"Failed to read Parquet from S3 client: {e}", exc_info=True)
        raise
    finally:
        logger.debug("Parquet S3 client reading attempt finished.")  

# ---------------------------------------------------------------------------
# Load helper
# ---------------------------------------------------------------------------
def _load_to_bronze(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Load a DataFrame into a table in the Bronze schema."""
    # TODO: Load the DataFrame into PostgreSQL using df.to_sql()
    # You'll need: get_engine(), and the right to_sql() parameters
    # Don't forget: index=False (we don't want the pandas index as a column)
    try:
        engine = get_engine()
        df.to_sql(
            name=table_name,
            con=engine,
            schema=BRONZE_SCHEMA,
            if_exists=if_exists,
            index=False,
        )
        # TODO (TP3): Replace with logger.info(...)
        logger.info(f"    ✅ {BRONZE_SCHEMA}.{table_name} — {len(df)} rows loaded")
    except Exception as e:
        logger.error(f"Failed to load table {table_name} to Bronze: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Extract functions — CSV datasets
# ---------------------------------------------------------------------------
def extract_products() -> pd.DataFrame:
    """Extract the product catalog from S3 → bronze.products."""
    # TODO (TP3): Replace print with logger.info, add try/except + logger.error + raise
    try:
        df = _read_csv_from_s3(f"{S3_PREFIX}/catalog/products.csv")
        logger.info(f"  📦 Products: {len(df)} rows, {len(df.columns)} columns")
        _load_to_bronze(df, "products")
        logger.info("Product catalog sucessfully exctracted from S3!")
        return df
    except Exception as e:
        logger.error(f"Failed to extract Product catalog from S3: {e}", exc_info=True)
        raise
    finally:
        logger.debug("Product catalog from S3 attempt extraction finished.")  


def extract_users() -> pd.DataFrame:
    """Extract users from S3 → bronze.users."""
    # TODO (TP3): Replace print with logger.info, add try/except + logger.error + raise
    try:
        df = _read_csv_from_s3(f"{S3_PREFIX}/users/users.csv")
        logger.info(f"  👤 Users: {len(df)} rows, {len(df.columns)} columns")
        _load_to_bronze(df, "users")
        logger.info("Users sucessfully exctracted from S3!")
        return df
    except Exception as e:
        logger.error(f"Failed to extract users catalog from S3: {e}", exc_info=True)
        raise
    finally:
        logger.debug("Users from S3 attempt extraction finished.")  


def extract_orders() -> pd.DataFrame:
    """Extract orders from S3 → bronze.orders."""
    # TODO (TP3): Replace print with logger.info, add try/except + logger.error + raise
    try:
        df = _read_csv_from_s3(f"{S3_PREFIX}/orders/orders.csv")
        logger.info(f"  🛍️ Orders: {len(df)} rows, {len(df.columns)} columns")
        _load_to_bronze(df, "orders")
        logger.info("Orders sucessfully exctracted from S3!")
        return df
    except Exception as e:
        logger.error(f"Failed to extract orders catalog from S3: {e}", exc_info=True)
        raise
    finally:
        logger.debug("Orders from S3 attempt extraction finished.")  


def extract_order_line_items() -> pd.DataFrame:
    """Extract order line items from S3 → bronze.order_line_items."""
    # TODO: Same pattern as extract_products()
    try:
        df = _read_csv_from_s3(f"{S3_PREFIX}/order_line_items/order_line_items.csv")
        logger.info(f"  📋 Line items: {len(df)} rows, {len(df.columns)} columns")
        _load_to_bronze(df, "order_line_items")
        logger.info("Order line items sucessfully exctracted from S3!")
        return df
    except Exception as e:
        logger.error(f"Failed to extract Order line items catalog from S3: {e}", exc_info=True)
        raise
    finally:
        logger.debug("Order line items from S3 attempt extraction finished.")  


# ---------------------------------------------------------------------------
# Extract functions — JSONL datasets
# ---------------------------------------------------------------------------
def extract_reviews() -> pd.DataFrame:
    """Extract customer reviews from S3 → bronze.reviews."""
    # TODO: Same pattern, but use _read_jsonl_from_s3() instead of _read_csv_from_s3()
    try:
        df = _read_jsonl_from_s3(f"{S3_PREFIX}/reviews/reviews.jsonl")
        logger.info(f"  ⭐ Reviews: {len(df)} rows, {len(df.columns)} columns")
        _load_to_bronze(df, "reviews")
        return df
    except Exception as e:
        logger.error(f"Failed to extract customer reviews catalog from S3: {e}", exc_info=True)
        raise
    finally:
        logger.debug("Customer reviews from S3 attempt extraction finished.") 


# ---------------------------------------------------------------------------
# Extract functions — Parquet datasets (partitioned)
# ---------------------------------------------------------------------------
def extract_clickstream() -> pd.DataFrame:
    """Extract clickstream events from S3 → bronze.clickstream."""
    # TODO: Same pattern, but use _read_partitioned_parquet_from_s3()
    # Note: pass a prefix (folder path), not a file key
    try: 
        df = _read_partitioned_parquet_from_s3(f"{S3_PREFIX}/clickstream/")
        logger.info(f"  🖱️ Clickstream: {len(df)} rows, {len(df.columns)} columns")
        _load_to_bronze(df, "clickstream")
        return df
    except Exception as e:
        logger.error(f"Failed to extract clickstream events reviews catalog from S3: {e}", exc_info=True)
        raise
    finally:
        logger.debug("Clickstream events from S3 attempt extraction finished.") 


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def extract_all() -> dict[str, pd.DataFrame]:
    """Run the complete extraction of all sources to Bronze."""
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"  🥉 EXTRACT → Bronze ({BRONZE_SCHEMA})")
        logger.info(f"{'='*60}\n")

        results = {}

        # TODO: Call each extract_*() function and store the result in the dict
        # There are 6 functions to call: 4 CSV + 1 JSONL + 1 Parquet

        # CSV datasets
        results["products"] = extract_products()
        results["users"] = extract_users()
        results["orders"] = extract_orders()
        results["order_line_items"] = extract_order_line_items()
        # JSONL datasets
        results["reviews"] = extract_reviews()
        # Parquet datasets
        results["clickstream"] = extract_clickstream()

        logger.info(f"\n  ✅ Extraction complete — {len(results)} tables loaded into {BRONZE_SCHEMA}")
        return results
    except Exception as e:
        logger.error(f"Critical error during the ETL process: {e}")
        raise


if __name__ == "__main__":
    extract_all()
