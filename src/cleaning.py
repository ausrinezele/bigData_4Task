from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


EARTH_RADIUS_KM = 6371.0088
MIN_MOVING_SOG_KNOTS = 0.5
MAX_REASONABLE_SOG_KNOTS = 80.0
MAX_IMPLIED_SPEED_KNOTS = 80.0
MIN_MOVEMENT_KM = 0.05
MAX_GAP_FOR_MOVEMENT_SECONDS = 30 * 60


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


def drop_duplicate_positions(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["mmsi", "timestamp", "latitude", "longitude"])


def add_previous_position_features(df: DataFrame) -> DataFrame:
    vessel_window = Window.partitionBy("mmsi").orderBy("timestamp")

    with_previous = (
        df.withColumn("prev_timestamp", F.lag("timestamp").over(vessel_window))
        .withColumn("prev_latitude", F.lag("latitude").over(vessel_window))
        .withColumn("prev_longitude", F.lag("longitude").over(vessel_window))
    )

    with_previous = with_previous.withColumn(
        "seconds_from_prev",
        F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long"),
    )

    with_previous = with_previous.withColumn(
        "distance_from_prev_km",
        F.when(
            F.col("prev_latitude").isNotNull() & F.col("prev_longitude").isNotNull(),
            haversine_between_km(
                F.col("latitude"),
                F.col("longitude"),
                F.col("prev_latitude"),
                F.col("prev_longitude"),
            ),
        ),
    )

    return with_previous.withColumn(
        "implied_speed_knots",
        F.when(
            F.col("seconds_from_prev") > 0,
            F.col("distance_from_prev_km")
            / (F.col("seconds_from_prev") / F.lit(3600.0))
            / F.lit(1.852),
        ),
    )


def filter_gps_noise(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("prev_timestamp").isNull()
        | (
            (F.col("seconds_from_prev") > 0)
            & (
                F.col("implied_speed_knots").isNull()
                | (F.col("implied_speed_knots") <= F.lit(MAX_IMPLIED_SPEED_KNOTS))
            )
        )
    )


def filter_moving_vessels(df: DataFrame) -> DataFrame:
    nav_status = F.lower(F.coalesce(F.col("nav_status").cast("string"), F.lit("")))

    stationary_status = (
        nav_status.contains("at anchor")
        | nav_status.contains("anchored")
        | nav_status.contains("moored")
        | nav_status.contains("aground")
        | nav_status.contains("not defined")
    )

    valid_sog = F.col("sog").isNull() | (
        (F.col("sog") >= F.lit(0.0))
        & (F.col("sog") <= F.lit(MAX_REASONABLE_SOG_KNOTS))
    )

    moving_by_sog = F.col("sog") >= F.lit(MIN_MOVING_SOG_KNOTS)
    moving_by_position = (
        (F.col("distance_from_prev_km") >= F.lit(MIN_MOVEMENT_KM))
        & (F.col("seconds_from_prev") <= F.lit(MAX_GAP_FOR_MOVEMENT_SECONDS))
    )

    return df.filter(valid_sog & ~stationary_status & (moving_by_sog | moving_by_position))


def clean_moving_area_records(df: DataFrame) -> DataFrame:
    deduped = drop_duplicate_positions(df)
    with_previous = add_previous_position_features(deduped)
    without_noise = filter_gps_noise(with_previous)
    return filter_moving_vessels(without_noise)
