import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    result into oleander.tutorial.retail_daily_product_funnel_df.

    The output table is expected to already exist.
    """
    start_date = F.lit(data_interval_start).cast("timestamp").cast("date")
    end_date = F.lit(data_interval_end).cast("timestamp").cast("date")

    product_funnel = (
        spark.table("oleander.tutorial.retail_session_products_df")
        .filter(F.col("event_date") >= start_date)
        .filter(F.col("event_date") < end_date)
        .groupBy(
            "event_date",
            "product_id",
            "product_name",
            "category",
        )
        .agg(
            F.sum(
                F.when(F.col("viewed_product"), F.lit(1))
                .otherwise(F.lit(0))
            ).alias("product_view_session_count"),
            F.sum(
                F.when(F.col("added_to_cart"), F.lit(1))
                .otherwise(F.lit(0))
            ).alias("add_to_cart_session_count"),
            F.sum(
                F.when(F.col("started_checkout"), F.lit(1))
                .otherwise(F.lit(0))
            ).alias("checkout_start_session_count"),
            F.sum(
                F.when(F.col("purchased"), F.lit(1))
                .otherwise(F.lit(0))
            ).alias("purchase_session_count"),
            F.sum("product_view_count").alias("product_view_event_count"),
            F.sum("add_to_cart_count").alias("add_to_cart_event_count"),
            F.sum("checkout_start_count").alias("checkout_start_event_count"),
            F.sum("purchase_count").alias("purchase_event_count"),
            F.sum("revenue").cast("decimal(18,2)").alias("revenue"),
        )
        .withColumn(
            "view_to_cart_rate",
            F.when(
                F.col("product_view_session_count") > F.lit(0),
                F.col("add_to_cart_session_count")
                / F.col("product_view_session_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "cart_to_checkout_rate",
            F.when(
                F.col("add_to_cart_session_count") > F.lit(0),
                F.col("checkout_start_session_count")
                / F.col("add_to_cart_session_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "checkout_to_purchase_rate",
            F.when(
                F.col("checkout_start_session_count") > F.lit(0),
                F.col("purchase_session_count")
                / F.col("checkout_start_session_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(9,4)"),
        )
        .withColumn(
            "view_to_purchase_rate",
            F.when(
                F.col("product_view_session_count") > F.lit(0),
                F.col("purchase_session_count")
                / F.col("product_view_session_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(9,4)"),
        )
        .select(
            "event_date",
            "product_id",
            "product_name",
            "category",
            "product_view_session_count",
            "add_to_cart_session_count",
            "checkout_start_session_count",
            "purchase_session_count",
            "product_view_event_count",
            "add_to_cart_event_count",
            "checkout_start_event_count",
            "purchase_event_count",
            "revenue",
            "view_to_cart_rate",
            "cart_to_checkout_rate",
            "checkout_to_purchase_rate",
            "view_to_purchase_rate",
        )
    )

    (
        product_funnel
        .writeTo("oleander.tutorial.retail_daily_product_funnel_df")
        .append()
    )


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tutorial-part09-dataframe")
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
