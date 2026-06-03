from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.config import CENTER_LAT, CENTER_LON, RADIUS_KM


EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat_col: Column, lon_col: Column) -> Column:
    """Distance in kilometers from each AIS point to the assignment center."""
    lat1 = F.radians(lat_col)
    lon1 = F.radians(lon_col)
    lat2 = F.radians(F.lit(CENTER_LAT))
    lon2 = F.radians(F.lit(CENTER_LON))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        F.pow(F.sin(dlat / 2.0), 2)
        + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2.0), 2)
    )
    return F.lit(2.0 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def filter_valid_coordinates(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("mmsi").isNotNull()
        & F.col("timestamp").isNotNull()
        & F.col("latitude").between(-90.0, 90.0)
        & F.col("longitude").between(-180.0, 180.0)
    )


def filter_assignment_area(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("distance_to_center_km", haversine_km(F.col("latitude"), F.col("longitude")))
        .filter(F.col("distance_to_center_km") <= F.lit(RADIUS_KM))
    )
