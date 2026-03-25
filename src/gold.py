"""
SOLUTION — Gold Layer (Business Aggregations)
"""

import pandas as pd
from sqlalchemy import text

from src.database import get_engine, SILVER_SCHEMA, GOLD_SCHEMA

# TODO (TP3): Import your logger and create a module-level logger
from src.logger import get_logger
logger = get_logger(__name__)


def _read_silver(table_name: str) -> pd.DataFrame:
    """Read a table from the Silver schema."""
    engine = get_engine()
    query = f"SELECT * FROM {SILVER_SCHEMA}.{table_name}"
    return pd.read_sql(query, engine)


def _create_gold_table(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Load a DataFrame into a table in the Gold schema."""
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=GOLD_SCHEMA,
        if_exists=if_exists,
        index=False,
    )
    # TODO (TP3): Replace with logger.info(...)
    logger.info(f"    ✅ {GOLD_SCHEMA}.{table_name} — {len(df)} rows")


def _create_gold_view(view_name: str, sql: str):
    """Create a SQL view in the Gold schema."""
    engine = get_engine()
    full_name = f"{GOLD_SCHEMA}.{view_name}"
    with engine.connect() as conn:
        conn.execute(text(f"DROP VIEW IF EXISTS {full_name}"))
        conn.execute(text(f"CREATE VIEW {full_name} AS {sql}"))
        conn.commit()
    # TODO (TP3): Replace with logger.info(...)
    logger.info(f"    ✅ View {full_name} created")


def create_daily_revenue():
    """Create gold.daily_revenue — daily revenue."""
    # TODO (TP3): Replace print with logger.info, add try/except + logger.error + raise
    try:
        logger.info("  📊 Gold: daily_revenue")

        # TODO: Create daily_revenue using SQL
        # Write a SQL query that joins fct_orders + fct_order_lines,
        # groups by date, and computes the aggregates described in the docstring.
        # Exclude cancelled/chargeback orders.
        # Then: pd.read_sql() → _create_gold_table()

        query = f"""
            SELECT
                DATE(o.order_date) AS order_date,
                COUNT(DISTINCT o.order_id) AS total_orders,
                ROUND(CAST(SUM(o.total_usd) AS numeric), 2) AS total_revenue,
                ROUND(CAST(AVG(o.total_usd) AS numeric), 2) AS avg_order_value,
                COALESCE(SUM(ol.quantity), 0) AS total_items
            FROM {SILVER_SCHEMA}.fct_orders o
            LEFT JOIN {SILVER_SCHEMA}.fct_order_lines ol ON o.order_id = ol.order_id
            WHERE o.status NOT IN ('cancelled', 'chargeback')
            GROUP BY DATE(o.order_date)
            ORDER BY order_date
        """
        df = pd.read_sql(query, get_engine())
        logger.info(f"Gold table daily_revenue created ({len(df)} rows)")
        _create_gold_table(df, "daily_revenue")
    except Exception as e:
        logger.error(f"Failed to create Gold table daily_revenue: {e}", exc_info=True)
        raise e


def create_product_performance():
    """Create gold.product_performance — metrics per product."""
    # TODO (TP3): Replace print with logger.info, add try/except + logger.error + raise
    try:
        logger.info("  🏆 Gold: product_performance")

        # TODO: Create the product_performance table
        # Join fct_order_lines with dim_products (and filter via fct_orders)
        # Group by product_id + product details, aggregate sales metrics
        # See the expected columns in the docstring above

        query = f"""
            SELECT
                ol.product_id,
                p.display_name AS product_name,
                p.brand,
                p.category,
                SUM(ol.quantity) AS total_quantity_sold,
                ROUND(CAST(SUM(ol.line_total_usd) AS numeric), 2) AS total_revenue,
                COUNT(DISTINCT ol.order_id) AS num_orders,
                ROUND(CAST(AVG(ol.unit_price_usd) AS numeric), 2) AS avg_unit_price
            FROM {SILVER_SCHEMA}.fct_order_lines ol
            INNER JOIN {SILVER_SCHEMA}.dim_products p ON ol.product_id = p.product_id
            INNER JOIN {SILVER_SCHEMA}.fct_orders o ON ol.order_id = o.order_id
            WHERE o.status NOT IN ('cancelled', 'chargeback')
            GROUP BY ol.product_id, p.display_name, p.brand, p.category
            ORDER BY total_revenue DESC
        """
        df = pd.read_sql(query, get_engine())
        logger.info(f"Gold table product_performance created ({len(df)} rows)")
        _create_gold_table(df, "product_performance")
    except Exception as e:
        logger.error(f"Failed to create Gold table product_performance: {e}", exc_info=True)
        raise e


def create_customer_ltv():
    """Create gold.customer_ltv — Lifetime Value per customer."""
    # TODO (TP3): Replace print with logger.info, add try/except + logger.error + raise
    try:
        logger.info("  💰 Gold: customer_ltv")

        # TODO: Create the customer_ltv table
        # Join fct_orders with dim_users
        # Group by customer, compute the aggregates listed in the docstring
        # Hint: MIN/MAX for dates, EXTRACT(DAY FROM ...) for tenure

        query = f"""
            SELECT
                u.user_id,
                u.email,
                u.first_name,
                u.last_name,
                u.loyalty_tier,
                COUNT(o.order_id) AS total_orders,
                ROUND(CAST(SUM(o.total_usd) AS numeric), 2) AS total_spent,
                ROUND(CAST(AVG(o.total_usd) AS numeric), 2) AS avg_order_value,
                MIN(o.order_date) AS first_order_date,
                MAX(o.order_date) AS last_order_date,
                EXTRACT(DAY FROM MAX(o.order_date) - MIN(o.order_date))::int AS days_as_customer
            FROM {SILVER_SCHEMA}.dim_users u
            INNER JOIN {SILVER_SCHEMA}.fct_orders o ON u.user_id = o.user_id
            WHERE o.status NOT IN ('cancelled', 'chargeback')
            GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.loyalty_tier
            HAVING COUNT(o.order_id) > 0
            ORDER BY total_spent DESC
        """
        df = pd.read_sql(query, get_engine())
        logger.info(f"Gold table customer_ltv created ({len(df)} rows)")
        _create_gold_table(df, "customer_ltv")
    except Exception as e:
        logger.error(f"Failed to create Gold table customer_ltv: {e}", exc_info=True)
        raise e


def create_gold_layer():
    """Create all tables/views for the Gold layer."""
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"  🥇 GOLD Layer ({GOLD_SCHEMA})")
        logger.info(f"{'='*60}\n")

        # TODO: Call each Gold creation function
        # There are 3 functions: daily_revenue, product_performance, customer_ltv
        create_daily_revenue()
        create_product_performance()
        create_customer_ltv()

        logger.info(f"\n  ✅ Gold layer created in {GOLD_SCHEMA}")
    except Exception as e:
        logger.error(f"Failed to create Gold table customer_ltv: {e}", exc_info=True)
        raise e
    finally:
        logger.info("Gold finished.")


if __name__ == "__main__":
    create_gold_layer()
