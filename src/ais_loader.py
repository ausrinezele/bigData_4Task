from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import AIS_INPUT_GLOB, END_TIME, START_TIME


RAW_COLUMN_MAP = {
    "# Timestamp": "timestamp_raw",
    "Timestamp": "timestamp_raw",
    "MMSI": "mmsi",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Navigational status": "nav_status",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "Name": "vessel_name",
    "Ship type": "ship_type",
}

REQUIRED_COLUMNS = [
    "timestamp",
    "mmsi",
    "latitude",
    "longitude",
    "nav_status",
    "sog",
    "cog",
    "heading",
    "vessel_name",
    "ship_type",
]


def load_raw_ais(spark: SparkSession, input_glob: str = AIS_INPUT_GLOB) -> DataFrame:
    """Load extracted Danish AIS CSV files recursively with Spark."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("recursiveFileLookup", True)
        .csv(input_glob)
    )


def normalize_columns(df: DataFrame) -> DataFrame:
    """Rename known Danish AIS columns into stable snake_case names."""
    for raw_name, clean_name in RAW_COLUMN_MAP.items():
        if raw_name in df.columns:
            df = df.withColumnRenamed(raw_name, clean_name)
    return df


def prepare_ais(df: DataFrame) -> DataFrame:
    """Parse types and keep only columns needed by the collision pipeline."""
    df = normalize_columns(df)

    prepared = (
        df.withColumn("timestamp", F.to_timestamp("timestamp_raw", "dd/MM/yyyy HH:mm:ss"))
        .withColumn("mmsi", F.col("mmsi").cast("long"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("sog", F.col("sog").cast("double"))
        .withColumn("cog", F.col("cog").cast("double"))
        .withColumn("heading", F.col("heading").cast("double"))
    )

    for column in REQUIRED_COLUMNS:
        if column not in prepared.columns:
            prepared = prepared.withColumn(column, F.lit(None))

    return prepared.select(*REQUIRED_COLUMNS)


def filter_december_2021(df: DataFrame) -> DataFrame:
    return df.filter((F.col("timestamp") >= F.lit(START_TIME)) & (F.col("timestamp") < F.lit(END_TIME)))


def load_december_ais(spark: SparkSession, input_glob: str = AIS_INPUT_GLOB) -> DataFrame:
    raw = load_raw_ais(spark, input_glob)
    prepared = prepare_ais(raw)
    return filter_december_2021(prepared)
