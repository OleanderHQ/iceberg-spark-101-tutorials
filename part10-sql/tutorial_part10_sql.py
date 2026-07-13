import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    return parser.parse_args()


def build_daily_product_cart_abandonment(
    spark: SparkSession,
    data_interval_start: str,
    data_interval_end: str,
) -> None:
    """
    Build daily product cart-abandonment rows for the given data interval.

    This function joins cart sessions with purchase sessions, summarizes the
    outcomes by event date and product, and inserts the result into
    oleander.tutorial.retail_daily_product_cart_abandonment.

    The output table is expected to already exist.
    """
    spark.sql(f"""
        INSERT INTO oleander.tutorial.retail_daily_product_cart_abandonment
        WITH filtered_session_products AS (
          SELECT *
          FROM oleander.tutorial.retail_session_products
          WHERE event_date >= CAST(CAST('{data_interval_start}' AS TIMESTAMP) AS DATE)
            AND event_date < CAST(CAST('{data_interval_end}' AS TIMESTAMP) AS DATE)
        ),
        cart_sessions AS (
          SELECT
            session_id,
            event_date,
            product_id,
            product_name,
            category
          FROM filtered_session_products
          WHERE added_to_cart
        ),
        purchase_sessions AS (
          SELECT
            session_id,
            product_id,
            TRUE AS purchased_cart
          FROM filtered_session_products
          WHERE purchased
        ),
        cart_outcomes AS (
          SELECT
            cart.event_date,
            cart.product_id,
            cart.product_name,
            cart.category,
            COALESCE(purchase.purchased_cart, FALSE) AS purchased_cart
          FROM cart_sessions AS cart
          LEFT JOIN purchase_sessions AS purchase
            ON cart.session_id = purchase.session_id
           AND cart.product_id = purchase.product_id
        ),
        cart_summary AS (
          SELECT
            event_date,
            product_id,
            product_name,
            category,
            COUNT(*) AS cart_session_count,
            SUM(CASE WHEN purchased_cart THEN 1 ELSE 0 END)
              AS purchased_cart_session_count,
            SUM(CASE WHEN NOT purchased_cart THEN 1 ELSE 0 END)
              AS abandoned_cart_session_count
          FROM cart_outcomes
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
          cart_session_count,
          purchased_cart_session_count,
          abandoned_cart_session_count,
          CAST(
            CASE
              WHEN cart_session_count > 0
                THEN abandoned_cart_session_count / cart_session_count
              ELSE 0
            END AS DECIMAL(9, 4)
          ) AS cart_abandonment_rate
        FROM cart_summary
    """)


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tutorial-part10-sql")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    build_daily_product_cart_abandonment(
        spark=spark,
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end,
    )

    spark.stop()


if __name__ == "__main__":
    main()
