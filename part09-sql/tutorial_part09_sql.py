import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    return parser.parse_args()


def build_daily_product_funnel(
    spark: SparkSession,
    data_interval_start: str,
    data_interval_end: str,
) -> None:
    """
    Build daily product funnel rows for the given data interval.

    This function groups session-product summaries by event date and product,
    calculates session and event counts for each funnel stage, and inserts the
    result into oleander.tutorial.retail_daily_product_funnel.

    The output table is expected to already exist.
    """
    spark.sql(f"""
        INSERT INTO oleander.tutorial.retail_daily_product_funnel
        WITH product_funnel AS (
          SELECT
            event_date,
            product_id,
            product_name,
            category,

            SUM(CASE WHEN viewed_product THEN 1 ELSE 0 END)
              AS product_view_session_count,
            SUM(CASE WHEN added_to_cart THEN 1 ELSE 0 END)
              AS add_to_cart_session_count,
            SUM(CASE WHEN started_checkout THEN 1 ELSE 0 END)
              AS checkout_start_session_count,
            SUM(CASE WHEN purchased THEN 1 ELSE 0 END)
              AS purchase_session_count,

            SUM(product_view_count) AS product_view_event_count,
            SUM(add_to_cart_count) AS add_to_cart_event_count,
            SUM(checkout_start_count) AS checkout_start_event_count,
            SUM(purchase_count) AS purchase_event_count,

            CAST(SUM(revenue) AS DECIMAL(18, 2)) AS revenue
          FROM oleander.tutorial.retail_session_products
          WHERE event_date >= CAST(CAST('{data_interval_start}' AS TIMESTAMP) AS DATE)
            AND event_date < CAST(CAST('{data_interval_end}' AS TIMESTAMP) AS DATE)
          GROUP BY
            event_date,
            product_id,
            product_name,
            category
        )
        SELECT
          event_date,
          product_id,
          product_name,
          category,

          product_view_session_count,
          add_to_cart_session_count,
          checkout_start_session_count,
          purchase_session_count,

          product_view_event_count,
          add_to_cart_event_count,
          checkout_start_event_count,
          purchase_event_count,

          revenue,

          CAST(
            CASE
              WHEN product_view_session_count > 0
                THEN add_to_cart_session_count / product_view_session_count
              ELSE 0
            END AS DECIMAL(9, 4)
          ) AS view_to_cart_rate,
          CAST(
            CASE
              WHEN add_to_cart_session_count > 0
                THEN checkout_start_session_count / add_to_cart_session_count
              ELSE 0
            END AS DECIMAL(9, 4)
          ) AS cart_to_checkout_rate,
          CAST(
            CASE
              WHEN checkout_start_session_count > 0
                THEN purchase_session_count / checkout_start_session_count
              ELSE 0
            END AS DECIMAL(9, 4)
          ) AS checkout_to_purchase_rate,
          CAST(
            CASE
              WHEN product_view_session_count > 0
                THEN purchase_session_count / product_view_session_count
              ELSE 0
            END AS DECIMAL(9, 4)
          ) AS view_to_purchase_rate
        FROM product_funnel
    """)


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tutorial-part09-sql")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    build_daily_product_funnel(
        spark=spark,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )

    spark.stop()


if __name__ == "__main__":
    main()
