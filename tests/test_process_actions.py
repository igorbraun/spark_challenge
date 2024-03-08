from spark_challenge.process_actions import (
    reduce_events_granularity_to_10_min,
    compute_avg_actions_number_per_window,
    compute_total_avg_actions_number,
    find_window_with_max_open_actions,
)
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    LongType,
    DoubleType,
)
from datetime import datetime
import pytest


spark = SparkSession.builder.getOrCreate()


EVENTS_SCHEMA = StructType(
    [
        StructField("time", TimestampType(), True),
        StructField("action", StringType(), True),
    ]
)
AGGREGATED_EVENTS_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("Close", LongType(), True),
        StructField("Open", LongType(), True),
    ]
)
AGGREGATED_EVENTS_WITH_AVG_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("Close", LongType(), True),
        StructField("Open", LongType(), True),
        StructField("avg_actions", DoubleType(), True),
    ]
)
TOTAL_AVG_SCHEMA = StructType(
    [
        StructField("avg(total_actions_per_10_min)", DoubleType(), True),
    ]
)


@pytest.mark.parametrize(
    "events,expected_aggregated_events",
    [
        (
            # normal case
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:01Z", "%Y-%m-%dT%H:%M:%SZ"),
                        "Close",
                    ),
                    (
                        datetime.strptime("2024-03-08T01:09:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        "Close",
                    ),
                    (
                        datetime.strptime("2024-03-08T01:11:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        "Open",
                    ),
                    (
                        datetime.strptime("2024-03-08T01:12:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        "Open",
                    ),
                ],
                schema=EVENTS_SCHEMA,
            ),
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        2,
                        0,
                    ),
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:20:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        0,
                        2,
                    ),
                ],
                schema=AGGREGATED_EVENTS_SCHEMA,
            ),
        ),
        (
            # Edge case: events exactly on border time
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        "Close",
                    ),
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        "Open",
                    ),
                ],
                schema=EVENTS_SCHEMA,
            ),
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        1,
                        0,
                    ),
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:20:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        0,
                        1,
                    ),
                ],
                schema=AGGREGATED_EVENTS_SCHEMA,
            ),
        ),
    ],
)
def test_reduce_events_granularity_to_10_min(events, expected_aggregated_events):
    aggregated_events = (
        reduce_events_granularity_to_10_min(events=events)
        .select(["window_start", "window_end", "Close", "Open"])
        .sort("window_start")
    )
    assertDataFrameEqual(aggregated_events, expected_aggregated_events)


@pytest.mark.parametrize(
    "aggregated_events,expected_aggregated_events_with_avg",
    [
        (
            # normal case
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        11,
                        147,
                    ),
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:20:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        19,
                        162,
                    ),
                ],
                schema=AGGREGATED_EVENTS_SCHEMA,
            ),
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        11,
                        147,
                        79.0,
                    ),
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:20:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        19,
                        162,
                        90.5,
                    ),
                ],
                schema=AGGREGATED_EVENTS_WITH_AVG_SCHEMA,
            ),
        ),
        (
            # edge case: zeros
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        0,
                        0,
                    ),
                ],
                schema=AGGREGATED_EVENTS_SCHEMA,
            ),
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        0,
                        0,
                        0.0,
                    ),
                ],
                schema=AGGREGATED_EVENTS_WITH_AVG_SCHEMA,
            ),
        ),
    ],
)
def test_compute_avg_actions_number_per_window(
    aggregated_events, expected_aggregated_events_with_avg
):
    aggregated_events_with_avg = compute_avg_actions_number_per_window(
        aggregated_events
    )
    assertDataFrameEqual(
        aggregated_events_with_avg, expected_aggregated_events_with_avg
    )


@pytest.mark.parametrize(
    "aggregated_events,expected_total_avg",
    [
        (
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        11,
                        147,
                    ),
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:20:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        19,
                        162,
                    ),
                ],
                schema=AGGREGATED_EVENTS_SCHEMA,
            ),
            spark.createDataFrame(
                data=[
                    (169.5,),
                ],
                schema=TOTAL_AVG_SCHEMA,
            ),
        ),
    ],
)
def test_compute_total_avg_actions_number(aggregated_events, expected_total_avg):
    total_avg_actions_number = compute_total_avg_actions_number(aggregated_events)
    assertDataFrameEqual(total_avg_actions_number, expected_total_avg)


@pytest.mark.parametrize(
    "aggregated_events,expected_max_open_actions_window",
    [
        (
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        11,
                        147,
                    ),
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:20:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        19,
                        162,
                    ),
                ],
                schema=AGGREGATED_EVENTS_SCHEMA,
            ),
            spark.createDataFrame(
                data=[
                    (
                        datetime.strptime("2024-03-08T01:10:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        datetime.strptime("2024-03-08T01:20:00Z", "%Y-%m-%dT%H:%M:%SZ"),
                        19,
                        162,
                    ),
                ],
                schema=AGGREGATED_EVENTS_SCHEMA,
            ),
        ),
    ],
)
def test_find_window_with_max_open_actions(
    aggregated_events, expected_max_open_actions_window
):
    max_open_actions_window = find_window_with_max_open_actions(aggregated_events)
    assertDataFrameEqual(max_open_actions_window, expected_max_open_actions_window)
