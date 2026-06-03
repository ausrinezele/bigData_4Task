from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


TRAJECTORY_WINDOW_SECONDS = 10 * 60
MIN_POINTS_IN_WINDOW = 5
MIN_POINTS_BEFORE = 2
MIN_POINTS_AFTER = 2


def add_event_ids(candidates: DataFrame) -> DataFrame:
    order_window = Window.orderBy(
        F.col("distance_meters").asc(),
        F.col("time_delta_seconds").asc(),
        F.col("event_timestamp").asc(),
    )
    return candidates.withColumn("event_id", F.row_number().over(order_window))


def select_validated_event(candidates: DataFrame, moving_df: DataFrame) -> DataFrame:
    candidates_with_ids = add_event_ids(candidates)

    candidate_vessels = candidates_with_ids.select(
        "event_id",
        "event_timestamp",
        F.lit("a").alias("side"),
        F.col("mmsi_a").alias("mmsi"),
    ).unionByName(
        candidates_with_ids.select(
            "event_id",
            "event_timestamp",
            F.lit("b").alias("side"),
            F.col("mmsi_b").alias("mmsi"),
        )
    )

    trajectory_points = (
        moving_df.select("mmsi", "timestamp", "latitude", "longitude")
        .join(F.broadcast(candidate_vessels), on="mmsi", how="inner")
        .withColumn("event_epoch", F.col("event_timestamp").cast("long"))
        .withColumn("point_epoch", F.col("timestamp").cast("long"))
        .filter(
            F.col("point_epoch").between(
                F.col("event_epoch") - F.lit(TRAJECTORY_WINDOW_SECONDS),
                F.col("event_epoch") + F.lit(TRAJECTORY_WINDOW_SECONDS),
            )
        )
    )

    support = trajectory_points.groupBy("event_id", "side").agg(
        F.count("*").alias("points_in_window"),
        F.sum(F.when(F.col("point_epoch") < F.col("event_epoch"), 1).otherwise(0)).alias(
            "points_before"
        ),
        F.sum(F.when(F.col("point_epoch") > F.col("event_epoch"), 1).otherwise(0)).alias(
            "points_after"
        ),
    )

    support_a = support.filter(F.col("side") == "a").select(
        "event_id",
        F.col("points_in_window").alias("points_a"),
        F.col("points_before").alias("points_before_a"),
        F.col("points_after").alias("points_after_a"),
    )
    support_b = support.filter(F.col("side") == "b").select(
        "event_id",
        F.col("points_in_window").alias("points_b"),
        F.col("points_before").alias("points_before_b"),
        F.col("points_after").alias("points_after_b"),
    )

    return (
        candidates_with_ids.join(support_a, on="event_id", how="left")
        .join(support_b, on="event_id", how="left")
        .fillna(
            0,
            subset=[
                "points_a",
                "points_before_a",
                "points_after_a",
                "points_b",
                "points_before_b",
                "points_after_b",
            ],
        )
        .filter(
            (F.col("points_a") >= F.lit(MIN_POINTS_IN_WINDOW))
            & (F.col("points_b") >= F.lit(MIN_POINTS_IN_WINDOW))
            & (F.col("points_before_a") >= F.lit(MIN_POINTS_BEFORE))
            & (F.col("points_after_a") >= F.lit(MIN_POINTS_AFTER))
            & (F.col("points_before_b") >= F.lit(MIN_POINTS_BEFORE))
            & (F.col("points_after_b") >= F.lit(MIN_POINTS_AFTER))
        )
        .orderBy(F.col("distance_meters").asc(), F.col("time_delta_seconds").asc())
        .limit(1)
    )
