import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    outcomes by event date and product, and merges the result into
    oleander.tutorial.retail_daily_product_cart_abandonment_df.

    The output table is expected to already exist.
    """
    start_date = F.lit(data_interval_start).cast("timestamp").cast("date")
    end_date = F.lit(data_interval_end).cast("timestamp").cast("date")

    session_products = (
        spark.table("oleander.tutorial.retail_session_products_df")
        .filter(F.col("event_date") >= start_date)
        .filter(F.col("event_date") < end_date)
    )

    cart_sessions = (
        session_products
        .filter(F.col("added_to_cart"))
        .select(
            "session_id",
            "event_date",
            "product_id",
            "product_name",
            "category",
        )
    )

    purchase_sessions = (
        session_products
        .filter(F.col("purchased"))
        .select(
            "session_id",
            "product_id",
            F.lit(True).alias("purchased_cart"),
        )
    )

    cart_outcomes = (
        cart_sessions
        .join(
            purchase_sessions,
            ["session_id", "product_id"],
            "left",
        )
        .withColumn(
            "purchased_cart",
            F.coalesce(F.col("purchased_cart"), F.lit(False)),
        )
    )

    cart_summary = (
        cart_outcomes
        .groupBy(
            "event_date",
            "product_id",
            "product_name",
            "category",
        )
        .agg(
            F.count(F.lit(1)).alias("cart_session_count"),
            F.sum(
                F.when(F.col("purchased_cart"), F.lit(1))
                .otherwise(F.lit(0))
            ).alias("purchased_cart_session_count"),
            F.sum(
                F.when(~F.col("purchased_cart"), F.lit(1))
                .otherwise(F.lit(0))
            ).alias("abandoned_cart_session_count"),
        )
        .withColumn(
            "cart_abandonment_rate",
            F.when(
                F.col("cart_session_count") > F.lit(0),
                F.col("abandoned_cart_session_count")
                / F.col("cart_session_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(9,4)"),
        )
        .select(
            "event_date",
            "product_id",
            "product_name",
            "category",
            "cart_session_count",
            "purchased_cart_session_count",
            "abandoned_cart_session_count",
            "cart_abandonment_rate",
        )
    )

    source_view = "daily_product_cart_abandonment_updates"
    cart_summary.createOrReplaceTempView(source_view)

    try:
        spark.sql(f"""
            MERGE INTO oleander.tutorial.retail_daily_product_cart_abandonment_df
              AS target
            USING {source_view} AS source
              ON target.event_date = source.event_date
             AND target.product_id = source.product_id
             AND target.product_name = source.product_name
             AND target.category = source.category
            WHEN MATCHED THEN UPDATE SET
              target.cart_session_count = source.cart_session_count,
              target.purchased_cart_session_count = source.purchased_cart_session_count,
              target.abandoned_cart_session_count = source.abandoned_cart_session_count,
              target.cart_abandonment_rate = source.cart_abandonment_rate
            WHEN NOT MATCHED THEN INSERT (
              event_date,
              product_id,
              product_name,
              category,
              cart_session_count,
              purchased_cart_session_count,
              abandoned_cart_session_count,
              cart_abandonment_rate
            ) VALUES (
              source.event_date,
              source.product_id,
              source.product_name,
              source.category,
              source.cart_session_count,
              source.purchased_cart_session_count,
              source.abandoned_cart_session_count,
              source.cart_abandonment_rate
            )
            WHEN NOT MATCHED BY SOURCE
              AND target.event_date >= CAST(CAST('{data_interval_start}' AS TIMESTAMP) AS DATE)
              AND target.event_date < CAST(CAST('{data_interval_end}' AS TIMESTAMP) AS DATE)
              THEN DELETE
        """)
    finally:
        spark.catalog.dropTempView(source_view)


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tutorial-part12-dataframe")
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
