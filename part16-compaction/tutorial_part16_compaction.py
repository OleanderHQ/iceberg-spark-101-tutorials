import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    return parser.parse_args()


def compact_clickstream_events(
    spark: SparkSession,
    data_interval_start: str,
    data_interval_end: str,
) -> None:
    """Compact eligible clickstream data files for the given interval."""
    spark.sql(f"""
        CALL oleander.system.rewrite_data_files(
          table => 'tutorial.retail_clickstream_events',
          strategy => 'binpack',
          options => map('min-input-files', '2'),
          where => 'event_time >= TIMESTAMP "{data_interval_start}"
                    AND event_time < TIMESTAMP "{data_interval_end}"'
        )
    """).show(truncate=False)


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("tutorial-part16-compaction")
        .getOrCreate()
    )

    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        compact_clickstream_events(
            spark=spark,
            data_interval_start=args.data_interval_start,
            data_interval_end=args.data_interval_end,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
