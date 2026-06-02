from pathlib import Path

from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    """Create the local Spark session used by the AIS processing pipeline."""
    return (
        SparkSession.builder.appName("ais-collision-detector")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    output_dir = Path("outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    try:
        print("AIS collision detector setup is ready.")
        print(f"Spark version: {spark.version}")
        print("Next step: add AIS CSV loading and filtering logic.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
