import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


KAFKA_BOOTSTRAP_ENV = "KAFKA_BOOTSTRAP"
KAFKA_TOPIC_ENV = "KAFKA_TOPIC"
APP_STATE_DIR_CONF = "spark.oleander.app.state.dir"
CHECKPOINT_DIRECTORY = "part15-retail-clickstream-events"
TARGET_TABLE = "oleander.tutorial.retail_clickstream_events"

EVENT_TYPES = (
    "page_view",
    "search",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase",
)

CLICKSTREAM_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("search_query", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("cart_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("traffic_source", StringType(), True),
    ]
)

OUTPUT_COLUMNS = (
    "event_id",
    "event_time",
    "ingestion_time",
    "user_id",
    "session_id",
    "event_type",
    "page_url",
    "referrer_url",
    "search_query",
    "product_id",
    "product_name",
    "category",
    "cart_id",
    "order_id",
    "quantity",
    "price",
    "device_type",
    "browser",
    "traffic_source",
)


def get_required_environment_variable(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"Required environment variable {name} is not set")
    return value.strip()


def get_checkpoint_location(spark: SparkSession) -> str:
    state_directory = spark.conf.get(APP_STATE_DIR_CONF, "").strip()
    if not state_directory:
        raise ValueError(
            f"Required Spark configuration {APP_STATE_DIR_CONF} is not set"
        )

    state_directory = state_directory.rstrip("/")
    return f"{state_directory}/{CHECKPOINT_DIRECTORY}"


def is_blank(column_name: str):
    column = F.col(column_name)
    return column.isNull() | (F.length(F.trim(column)) == F.lit(0))


def build_validation_error():
    product_event_types = (
        "product_view",
        "add_to_cart",
        "remove_from_cart",
        "checkout_start",
        "purchase",
    )
    cart_event_types = (
        "add_to_cart",
        "remove_from_cart",
        "checkout_start",
        "purchase",
    )

    validation_error = F.when(
        is_blank("event_id"),
        F.lit("event_id is required"),
    )

    for column_name in ("user_id", "session_id", "event_type", "page_url"):
        validation_error = validation_error.when(
            is_blank(column_name),
            F.lit(f"{column_name} is required"),
        )

    validation_error = validation_error.when(
        F.col("event_time").isNull(),
        F.lit("event_time is required"),
    )

    for column_name in ("device_type", "browser", "traffic_source"):
        validation_error = validation_error.when(
            is_blank(column_name),
            F.lit(f"{column_name} is required"),
        )

    validation_error = validation_error.when(
        ~F.col("event_type").isin(*EVENT_TYPES),
        F.lit("event_type is not supported"),
    ).when(
        (F.col("event_type") == F.lit("search")) & is_blank("search_query"),
        F.lit("search_query is required for search events"),
    )

    for column_name in ("product_id", "product_name", "category"):
        validation_error = validation_error.when(
            F.col("event_type").isin(*product_event_types) & is_blank(column_name),
            F.lit(f"{column_name} is required for product events"),
        )

    validation_error = (
        validation_error.when(
            F.col("event_type").isin(*product_event_types)
            & (F.col("price").isNull() | (F.col("price") < F.lit(0))),
            F.lit("price must be nonnegative for product events"),
        )
        .when(
            F.col("event_type").isin(*cart_event_types) & is_blank("cart_id"),
            F.lit("cart_id is required for cart events"),
        )
        .when(
            F.col("event_type").isin(*cart_event_types)
            & (
                F.col("quantity").isNull()
                | (F.col("quantity") <= F.lit(0))
            ),
            F.lit("quantity must be positive for cart events"),
        )
        .when(
            (F.col("event_type") == F.lit("purchase")) & is_blank("order_id"),
            F.lit("order_id is required for purchase events"),
        )
        .otherwise(F.lit(None).cast("string"))
    )

    return validation_error


def parse_and_validate_events(kafka_events: DataFrame) -> DataFrame:
    parsed_events = (
        kafka_events.select(
            F.from_json(
                F.col("value").cast("string"),
                CLICKSTREAM_EVENT_SCHEMA,
                {"mode": "FAILFAST"},
            ).alias("event")
        )
        .select("event.*")
        .withColumn("_validation_error", build_validation_error())
    )

    validated_events = (
        parsed_events.withColumn(
            "event_id",
            F.when(
                F.col("_validation_error").isNotNull(),
                F.raise_error(
                    F.concat(
                        F.lit("Invalid clickstream event: "),
                        F.col("_validation_error"),
                    )
                ),
            ).otherwise(F.col("event_id")),
        )
        .drop("_validation_error")
        .withColumn("ingestion_time", F.current_timestamp())
    )

    return validated_events.select(*OUTPUT_COLUMNS)


def read_kafka_events(
    spark: SparkSession,
    kafka_bootstrap: str,
    kafka_topic: str,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def start_clickstream_ingestion(
    events: DataFrame,
    checkpoint_location: str,
) -> StreamingQuery:
    return (
        events.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", checkpoint_location)
        .option("fanout-enabled", "true")
        .queryName("part15-retail-clickstream-events")
        .toTable(TARGET_TABLE)
    )


def main() -> None:
    kafka_bootstrap = get_required_environment_variable(KAFKA_BOOTSTRAP_ENV)
    kafka_topic = get_required_environment_variable(KAFKA_TOPIC_ENV)

    spark = (
        SparkSession.builder.appName("tutorial-part15-streaming").getOrCreate()
    )
    streaming = None

    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        checkpoint_location = get_checkpoint_location(spark)
        kafka_events = read_kafka_events(spark, kafka_bootstrap, kafka_topic)
        clickstream_events = parse_and_validate_events(kafka_events)
        streaming = start_clickstream_ingestion(
            clickstream_events,
            checkpoint_location,
        )
        streaming.awaitTermination()
    finally:
        if streaming is not None and streaming.isActive:
            streaming.stop()
        spark.stop()


if __name__ == "__main__":
    main()
