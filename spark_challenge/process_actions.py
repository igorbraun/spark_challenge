from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import col, window
import argparse

spark = SparkSession.builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-events_path",
        type=str,
        default="/home/igor/spark_challenge/data/events.csv",
    )
    args = parser.parse_args()

    events = read_event_data(events_path=args.events_path)

    print("Aggregated events:")
    aggregated_events = (
        reduce_events_granularity_to_10_min(events).sort("window_start").cache()
    )
    aggregated_events.show(5)
    """
    +-----+----+-------------------+-------------------+
    |Close|Open|       window_start|         window_end|
    +-----+----+-------------------+-------------------+
    |    0|  32|2016-07-26 04:40:00|2016-07-26 04:50:00|
    |   11| 147|2016-07-26 04:50:00|2016-07-26 05:00:00|
    |   19| 162|2016-07-26 05:00:00|2016-07-26 05:10:00|
    """

    print("Row-wise average actions number:")
    row_wise_avg_actions_number = compute_avg_actions_number_per_window(
        aggregated_events
    )
    row_wise_avg_actions_number.show(5)
    """
    +-----+----+-------------------+-------------------+-----------+
    |Close|Open|       window_start|         window_end|avg_actions|
    +-----+----+-------------------+-------------------+-----------+
    |    0|  32|2016-07-26 04:40:00|2016-07-26 04:50:00|       16.0|
    |   11| 147|2016-07-26 04:50:00|2016-07-26 05:00:00|       79.0|
    |   19| 162|2016-07-26 05:00:00|2016-07-26 05:10:00|       90.5|
    """

    print("Total average actions number:")
    total_avg_actions_number = compute_total_avg_actions_number(aggregated_events)
    total_avg_actions_number.show()
    """
    +-----------------------------+
    |avg(total_actions_per_10_min)|
    +-----------------------------+
    |            319.4888178913738|
    +-----------------------------+
    """

    print("Row with maximum of open actions:")
    max_open_actions_window = find_window_with_max_open_actions(aggregated_events)
    max_open_actions_window.show()
    """
    print(max_open_actions_window.show())
    +-----+----+-------------------+-------------------+
    |Close|Open|       window_start|         window_end|
    +-----+----+-------------------+-------------------+
    |  189| 185|2016-07-26 22:10:00|2016-07-26 22:20:00|
    +-----+----+-------------------+-------------------+
    """


def read_event_data(events_path: str) -> DataFrame:
    """Reads and returns the events"""
    events_schema = StructType(
        [
            StructField("time", TimestampType(), True),
            StructField("action", StringType(), True),
        ]
    )
    return spark.read.csv(path=events_path, header=True, schema=events_schema)


def reduce_events_granularity_to_10_min(events: DataFrame) -> DataFrame:
    """Groups events into 10-minute windows and counts actions of each type"""
    aggregated_events = (
        events.groupBy(window("time", "10 minutes")).pivot("action").count().fillna(0)
    )
    return (
        aggregated_events.withColumn("window_start", aggregated_events.window.start)
        .withColumn("window_end", aggregated_events.window.end)
        .drop("window")
    )


def compute_avg_actions_number_per_window(aggregated_events: DataFrame) -> DataFrame:
    """Computes the row-wise average number of actions for each 10 minutes window"""
    return aggregated_events.withColumn(
        "avg_actions", ((col("Close") + col("Open")) / 2)
    )


def compute_total_avg_actions_number(aggregated_events: DataFrame) -> DataFrame:
    """Computes the total average number of all 10 min windows"""
    aggregated_events_with_total_actions = aggregated_events.withColumn(
        "total_actions_per_10_min", col("Close") + col("Open")
    )
    return aggregated_events_with_total_actions.agg({"total_actions_per_10_min": "avg"})


def find_window_with_max_open_actions(aggregated_events: DataFrame) -> DataFrame:
    """Returns the 10 minutes window with the max number of open actions"""
    return aggregated_events.orderBy(col("Open").desc()).limit(1)


if __name__ == "__main__":
    main()
