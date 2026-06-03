from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.config import (
    CENTER_LAT,
    CENTER_LON,
    COLLISION_DISTANCE_THRESHOLD_METERS,
    SPATIAL_GRID_METERS,
    TIME_BUCKET_SECONDS,
    TIME_TOLERANCE_SECONDS,
    TOP_CANDIDATE_LIMIT,
)


EARTH_RADIUS_KM = 6371.0088
KM_PER_LATITUDE_DEGREE = 110.574
KM_PER_LONGITUDE_DEGREE = 111.320


def haversine_between_km(
    lat_a: Column,
    lon_a: Column,
    lat_b: Column,
    lon_b: Column,
) -> Column:
    lat1 = F.radians(lat_a)
    lon1 = F.radians(lon_a)
    lat2 = F.radians(lat_b)
    lon2 = F.radians(lon_b)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        F.pow(F.sin(dlat / 2.0), 2)
        + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2.0), 2)
    )
    return F.lit(2.0 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def add_spatiotemporal_buckets(df: DataFrame) -> DataFrame:
    grid_km = SPATIAL_GRID_METERS / 1000.0
    lon_km_scale = KM_PER_LONGITUDE_DEGREE * F.cos(F.radians(F.lit(CENTER_LAT)))

    return (
        df.withColumn(
            "time_bucket",
            F.floor(F.col("timestamp").cast("long") / F.lit(TIME_BUCKET_SECONDS)).cast("long"),
        )
        .withColumn("x_km", (F.col("longitude") - F.lit(CENTER_LON)) * lon_km_scale)
        .withColumn("y_km", (F.col("latitude") - F.lit(CENTER_LAT)) * F.lit(KM_PER_LATITUDE_DEGREE))
        .withColumn("x_cell", F.floor(F.col("x_km") / F.lit(grid_km)).cast("long"))
        .withColumn("y_cell", F.floor(F.col("y_km") / F.lit(grid_km)).cast("long"))
    )


def expand_neighbor_buckets(df: DataFrame) -> DataFrame:
    time_offsets = F.array(*[F.lit(offset) for offset in (-1, 0, 1)])
    cell_offsets = F.array(*[F.lit(offset) for offset in (-1, 0, 1)])

    return (
        df.withColumn("time_offset", F.explode(time_offsets))
        .withColumn("x_offset", F.explode(cell_offsets))
        .withColumn("y_offset", F.explode(cell_offsets))
        .withColumn("join_time_bucket", F.col("time_bucket") + F.col("time_offset"))
        .withColumn("join_x_cell", F.col("x_cell") + F.col("x_offset"))
        .withColumn("join_y_cell", F.col("y_cell") + F.col("y_offset"))
        .drop("time_offset", "x_offset", "y_offset")
    )


def find_close_encounters(df: DataFrame) -> DataFrame:
    bucketed = add_spatiotemporal_buckets(df).select(
        "mmsi",
        "vessel_name",
        "timestamp",
        "latitude",
        "longitude",
        "sog",
        "cog",
        "time_bucket",
        "x_cell",
        "y_cell",
    )

    left = bucketed.select(
        F.col("mmsi").alias("mmsi_a"),
        F.col("vessel_name").alias("vessel_name_a"),
        F.col("timestamp").alias("timestamp_a"),
        F.col("latitude").alias("latitude_a"),
        F.col("longitude").alias("longitude_a"),
        F.col("sog").alias("sog_a"),
        F.col("cog").alias("cog_a"),
        F.col("time_bucket").alias("join_time_bucket"),
        F.col("x_cell").alias("join_x_cell"),
        F.col("y_cell").alias("join_y_cell"),
    )

    right = expand_neighbor_buckets(bucketed).select(
        F.col("mmsi").alias("mmsi_b"),
        F.col("vessel_name").alias("vessel_name_b"),
        F.col("timestamp").alias("timestamp_b"),
        F.col("latitude").alias("latitude_b"),
        F.col("longitude").alias("longitude_b"),
        F.col("sog").alias("sog_b"),
        F.col("cog").alias("cog_b"),
        "join_time_bucket",
        "join_x_cell",
        "join_y_cell",
    )

    candidates = left.join(
        right,
        on=["join_time_bucket", "join_x_cell", "join_y_cell"],
        how="inner",
    ).filter(F.col("mmsi_a") < F.col("mmsi_b"))

    candidates = candidates.withColumn(
        "time_delta_seconds",
        F.abs(F.col("timestamp_a").cast("long") - F.col("timestamp_b").cast("long")),
    ).filter(F.col("time_delta_seconds") <= F.lit(TIME_TOLERANCE_SECONDS))

    candidates = candidates.withColumn(
        "distance_meters",
        haversine_between_km(
            F.col("latitude_a"),
            F.col("longitude_a"),
            F.col("latitude_b"),
            F.col("longitude_b"),
        )
        * F.lit(1000.0),
    ).filter(F.col("distance_meters") <= F.lit(COLLISION_DISTANCE_THRESHOLD_METERS))

    return (
        candidates.withColumn(
            "event_timestamp",
            F.least(F.col("timestamp_a"), F.col("timestamp_b")),
        )
        .withColumn("event_latitude", (F.col("latitude_a") + F.col("latitude_b")) / F.lit(2.0))
        .withColumn("event_longitude", (F.col("longitude_a") + F.col("longitude_b")) / F.lit(2.0))
        .select(
            "mmsi_a",
            "vessel_name_a",
            "mmsi_b",
            "vessel_name_b",
            "event_timestamp",
            "timestamp_a",
            "timestamp_b",
            "time_delta_seconds",
            "event_latitude",
            "event_longitude",
            "latitude_a",
            "longitude_a",
            "latitude_b",
            "longitude_b",
            "distance_meters",
            "sog_a",
            "sog_b",
            "cog_a",
            "cog_b",
        )
        .orderBy(F.col("distance_meters").asc(), F.col("time_delta_seconds").asc())
        .limit(TOP_CANDIDATE_LIMIT)
    )
