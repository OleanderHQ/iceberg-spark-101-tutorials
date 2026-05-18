import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    oleander.tutorial.retail_daily_traffic_summary_df.

    The output table is expected to already exist.
    """
    (
        spark.table("oleander.tutorial.retail_clickstream_events")
        .filter(F.col("event_time") >= F.lit(data_interval_start).cast("timestamp"))
        .filter(F.col("event_time") < F.lit(data_interval_end).cast("timestamp"))
        .groupBy(F.col("event_time").cast("date").alias("event_date"))
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.sum(
                F.when(F.col("event_type") == "page_view", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("page_view_count"),
            F.sum(
                F.when(F.col("event_type") == "search", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("search_count"),
            F.sum(
                F.when(F.col("event_type") == "product_view", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("product_view_count"),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("add_to_cart_count"),
            F.sum(
                F.when(F.col("event_type") == "remove_from_cart", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("remove_from_cart_count"),
            F.sum(
                F.when(F.col("event_type") == "checkout_start", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("checkout_start_count"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("purchase_count"),
        )
        .select(
            "event_date",
            "total_events",
            "unique_users",
            "unique_sessions",
            "page_view_count",
            "search_count",
            "product_view_count",
            "add_to_cart_count",
            "remove_from_cart_count",
            "checkout_start_count",
            "purchase_count",
        )
        .writeTo("oleander.tutorial.retail_daily_traffic_summary_df")
        .append()
    )


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
    oleander.tutorial.retail_sessions_df.

    The output table is expected to already exist.
    """
    (
        spark.table("oleander.tutorial.retail_clickstream_events")
        .filter(F.col("event_time") >= F.lit(data_interval_start).cast("timestamp"))
        .filter(F.col("event_time") < F.lit(data_interval_end).cast("timestamp"))
        .groupBy("session_id", "user_id")
        .agg(
            F.min("event_time").cast("date").alias("session_date"),
            F.min("event_time").alias("session_start_time"),
            F.max("event_time").alias("session_end_time"),
            (
                F.unix_timestamp(F.max("event_time"))
                - F.unix_timestamp(F.min("event_time"))
            ).alias("session_duration_seconds"),
            F.count(F.lit(1)).alias("event_count"),
            F.sum(
                F.when(F.col("event_type") == "page_view", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("page_view_count"),
            F.sum(
                F.when(F.col("event_type") == "search", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("search_count"),
            F.sum(
                F.when(F.col("event_type") == "product_view", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("product_view_count"),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("add_to_cart_count"),
            F.sum(
                F.when(F.col("event_type") == "remove_from_cart", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("remove_from_cart_count"),
            F.sum(
                F.when(F.col("event_type") == "checkout_start", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("checkout_start_count"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("purchase_count"),
            (
                F.sum(
                    F.when(F.col("event_type") == "search", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("searched"),
            (
                F.sum(
                    F.when(F.col("event_type") == "product_view", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("viewed_product"),
            (
                F.sum(
                    F.when(F.col("event_type") == "add_to_cart", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("added_to_cart"),
            (
                F.sum(
                    F.when(F.col("event_type") == "checkout_start", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("started_checkout"),
            (
                F.sum(
                    F.when(F.col("event_type") == "purchase", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("purchased"),
        )
        .select(
            "session_id",
            "user_id",
            "session_date",
            "session_start_time",
            "session_end_time",
            "session_duration_seconds",
            "event_count",
            "page_view_count",
            "search_count",
            "product_view_count",
            "add_to_cart_count",
            "remove_from_cart_count",
            "checkout_start_count",
            "purchase_count",
            "searched",
            "viewed_product",
            "added_to_cart",
            "started_checkout",
            "purchased",
        )
        .writeTo("oleander.tutorial.retail_sessions_df")
        .append()
    )


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

    The result is inserted into oleander.tutorial.retail_session_products_df.

    The output table is expected to already exist.
    """
    (
        spark.table("oleander.tutorial.retail_clickstream_events")
        .filter(F.col("event_time") >= F.lit(data_interval_start).cast("timestamp"))
        .filter(F.col("event_time") < F.lit(data_interval_end).cast("timestamp"))
        .filter(F.col("product_id").isNotNull())
        .groupBy(
            "session_id",
            "user_id",
            "product_id",
            "product_name",
            "category",
        )
        .agg(
            F.min("event_time").cast("date").alias("event_date"),
            F.min(
                F.when(
                    F.col("event_type") == "product_view",
                    F.col("event_time"),
                )
            ).alias("first_product_view_time"),
            F.min(
                F.when(
                    F.col("event_type") == "add_to_cart",
                    F.col("event_time"),
                )
            ).alias("first_add_to_cart_time"),
            F.min(
                F.when(
                    F.col("event_type") == "remove_from_cart",
                    F.col("event_time"),
                )
            ).alias("first_remove_from_cart_time"),
            F.min(
                F.when(
                    F.col("event_type") == "checkout_start",
                    F.col("event_time"),
                )
            ).alias("first_checkout_start_time"),
            F.min(
                F.when(
                    F.col("event_type") == "purchase",
                    F.col("event_time"),
                )
            ).alias("first_purchase_time"),
            F.sum(
                F.when(F.col("event_type") == "product_view", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("product_view_count"),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("add_to_cart_count"),
            F.sum(
                F.when(F.col("event_type") == "remove_from_cart", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("remove_from_cart_count"),
            F.sum(
                F.when(F.col("event_type") == "checkout_start", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("checkout_start_count"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.lit(1))
                .otherwise(F.lit(0))
            ).alias("purchase_count"),
            (
                F.sum(
                    F.when(F.col("event_type") == "product_view", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("viewed_product"),
            (
                F.sum(
                    F.when(F.col("event_type") == "add_to_cart", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("added_to_cart"),
            (
                F.sum(
                    F.when(F.col("event_type") == "remove_from_cart", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("removed_from_cart"),
            (
                F.sum(
                    F.when(F.col("event_type") == "checkout_start", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("started_checkout"),
            (
                F.sum(
                    F.when(F.col("event_type") == "purchase", F.lit(1))
                    .otherwise(F.lit(0))
                )
                > F.lit(0)
            ).alias("purchased"),
            F.sum(
                F.when(
                    F.col("event_type") == "add_to_cart",
                    F.coalesce(F.col("quantity"), F.lit(0)),
                )
                .otherwise(F.lit(0))
            ).alias("quantity_added"),
            F.sum(
                F.when(
                    F.col("event_type") == "purchase",
                    F.coalesce(F.col("quantity"), F.lit(0)),
                )
                .otherwise(F.lit(0))
            ).alias("quantity_purchased"),
            F.sum(
                F.when(
                    F.col("event_type") == "purchase",
                    F.coalesce(F.col("quantity"), F.lit(0))
                    * F.coalesce(
                        F.col("price"),
                        F.lit("0.00").cast("decimal(10,2)"),
                    ),
                )
                .otherwise(F.lit("0.00").cast("decimal(10,2)"))
            )
            .cast("decimal(12,2)")
            .alias("revenue"),
        )
        .select(
            "session_id",
            "user_id",
            "event_date",
            "product_id",
            "product_name",
            "category",
            "first_product_view_time",
            "first_add_to_cart_time",
            "first_remove_from_cart_time",
            "first_checkout_start_time",
            "first_purchase_time",
            "product_view_count",
            "add_to_cart_count",
            "remove_from_cart_count",
            "checkout_start_count",
            "purchase_count",
            "viewed_product",
            "added_to_cart",
            "removed_from_cart",
            "started_checkout",
            "purchased",
            "quantity_added",
            "quantity_purchased",
            "revenue",
        )
        .writeTo("oleander.tutorial.retail_session_products_df")
        .append()
    )


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tutorial-part08-dataframe")
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
