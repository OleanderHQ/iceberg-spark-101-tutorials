import argparse
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    return parser.parse_args()


def build_daily_traffic_summary(
    spark: SparkSession,
    data_interval_start: str,
    data_interval_end: str,
) -> None:
    """
    Build daily traffic summary rows for the given data interval.

    This function reads raw clickstream events from
    oleander.tutorial.retail_clickstream_events, groups them by event date,
    and inserts one summary row per day into
    oleander.tutorial.retail_daily_traffic_summary.

    The output table is expected to already exist.
    """
    spark.sql(f"""
        INSERT INTO oleander.tutorial.retail_daily_traffic_summary
        SELECT
          CAST(event_time AS DATE) AS event_date,
          COUNT(*) AS total_events,
          COUNT(DISTINCT user_id) AS unique_users,
          COUNT(DISTINCT session_id) AS unique_sessions,
          SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
          SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_count,
          SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_view_count,
          SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
          SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_count,
          SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS checkout_start_count,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
        FROM oleander.tutorial.retail_clickstream_events
        WHERE event_time >= TIMESTAMP '{data_interval_start}'
          AND event_time <  TIMESTAMP '{data_interval_end}'
        GROUP BY CAST(event_time AS DATE)
    """)


def build_sessions(
    spark: SparkSession,
    data_interval_start: str,
    data_interval_end: str,
) -> None:
    """
    Build session-level summary rows for the given data interval.

    This function groups raw clickstream events by session_id and user_id,
    calculates session start/end time, duration, event counts, and boolean
    behavior flags, then inserts the result into
    oleander.tutorial.retail_sessions.

    The output table is expected to already exist.
    """
    spark.sql(f"""
        INSERT INTO oleander.tutorial.retail_sessions
        SELECT
          session_id,
          user_id,
          CAST(MIN(event_time) AS DATE) AS session_date,
          MIN(event_time) AS session_start_time,
          MAX(event_time) AS session_end_time,
          unix_timestamp(MAX(event_time)) - unix_timestamp(MIN(event_time))
            AS session_duration_seconds,
          COUNT(*) AS event_count,
          SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
          SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_count,
          SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_view_count,
          SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
          SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_count,
          SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS checkout_start_count,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
          SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) > 0 AS searched,
          SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) > 0 AS viewed_product,
          SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) > 0 AS added_to_cart,
          SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) > 0 AS started_checkout,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) > 0 AS purchased
        FROM oleander.tutorial.retail_clickstream_events
        WHERE event_time >= TIMESTAMP '{data_interval_start}'
          AND event_time <  TIMESTAMP '{data_interval_end}'
        GROUP BY
          session_id,
          user_id
    """)


def build_session_products(
    spark: SparkSession,
    data_interval_start: str,
    data_interval_end: str,
) -> None:
    """
    Build session-product-level summary rows for the given data interval.

    This function groups product-related clickstream events by session, user,
    and product. It creates one row per session-product pair, including event
    counts, first event timestamps, behavior flags, quantities, and revenue.

    The result is inserted into oleander.tutorial.retail_session_products.

    The output table is expected to already exist.
    """
    spark.sql(f"""
        INSERT INTO oleander.tutorial.retail_session_products
        SELECT
          session_id,
          user_id,
    
          CAST(MIN(event_time) AS DATE) AS event_date,
    
          product_id,
          product_name,
          category,
    
          MIN(CASE WHEN event_type = 'product_view' THEN event_time END) AS first_product_view_time,
          MIN(CASE WHEN event_type = 'add_to_cart' THEN event_time END) AS first_add_to_cart_time,
          MIN(CASE WHEN event_type = 'remove_from_cart' THEN event_time END) AS first_remove_from_cart_time,
          MIN(CASE WHEN event_type = 'checkout_start' THEN event_time END) AS first_checkout_start_time,
          MIN(CASE WHEN event_type = 'purchase' THEN event_time END) AS first_purchase_time,
    
          SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_view_count,
          SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
          SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_count,
          SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS checkout_start_count,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    
          SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) > 0 AS viewed_product,
          SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) > 0 AS added_to_cart,
          SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) > 0 AS removed_from_cart,
          SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) > 0 AS started_checkout,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) > 0 AS purchased,
    
          SUM(
            CASE
              WHEN event_type = 'add_to_cart' THEN COALESCE(quantity, 0)
              ELSE 0
            END
          ) AS quantity_added,
    
          SUM(
            CASE
              WHEN event_type = 'purchase' THEN COALESCE(quantity, 0)
              ELSE 0
            END
          ) AS quantity_purchased,
    
          CAST(
            SUM(
              CASE
                WHEN event_type = 'purchase'
                  THEN COALESCE(quantity, 0) * COALESCE(price, CAST(0.00 AS DECIMAL(10, 2)))
                ELSE CAST(0.00 AS DECIMAL(10, 2))
              END
            ) AS DECIMAL(12, 2)
          ) AS revenue
    
        FROM oleander.tutorial.retail_clickstream_events
        WHERE event_time >= TIMESTAMP '{data_interval_start}'
          AND event_time <  TIMESTAMP '{data_interval_end}'
          AND product_id IS NOT NULL
        GROUP BY
          session_id,
          user_id,
          product_id,
          product_name,
          category
    """)


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tutorial-part08-sql")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    build_daily_traffic_summary(
        spark=spark,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )

    build_sessions(
        spark=spark,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )

    build_session_products(
        spark=spark,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )

    spark.stop()


if __name__ == "__main__":
    main()
