from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from src.ais_loader import load_december_ais
from src.cleaning import clean_moving_area_records
from src.collision_detection import find_close_encounters
from src.config import AIS_INPUT_GLOB, CENTER_LAT, CENTER_LON, RADIUS_KM, RADIUS_NAUTICAL_MILES
from src.spatial_filter import filter_assignment_area, filter_valid_coordinates
from src.validation import select_validated_event
from src.visualization import extract_trajectory_window, format_event_summary, save_trajectory_outputs


def build_spark() -> SparkSession:
    """Create the local Spark session used by the AIS processing pipeline."""
    return (
        SparkSession.builder.appName("ais-collision-detector")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    output_dir = Path("outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    try:
        print("AIS collision detector starting.")
        print(f"Spark version: {spark.version}")
        print(f"Reading AIS CSV files from: {AIS_INPUT_GLOB}")

        ais_df = load_december_ais(spark)
        row_count = ais_df.count()
        vessel_count = ais_df.select("mmsi").distinct().count()

        print(f"Rows after December 2021 filter: {row_count:,}")
        print(f"Distinct MMSI values: {vessel_count:,}")

        valid_df = filter_valid_coordinates(ais_df)
        valid_count = valid_df.count()

        area_df = filter_assignment_area(valid_df).persist(StorageLevel.DISK_ONLY)
        area_count = area_df.count()
        area_vessel_count = area_df.select("mmsi").distinct().count()

        print(f"Rows after coordinate validation: {valid_count:,}")
        print(
            "Rows inside "
            f"{RADIUS_NAUTICAL_MILES:.0f} nm / {RADIUS_KM:.1f} km "
            f"of ({CENTER_LAT}, {CENTER_LON}): {area_count:,}"
        )
        print(f"Distinct MMSI values inside area: {area_vessel_count:,}")

        moving_df = clean_moving_area_records(area_df).persist(StorageLevel.DISK_ONLY)
        moving_count = moving_df.count()
        moving_vessel_count = moving_df.select("mmsi").distinct().count()

        print(f"Rows after stationary/noise filtering: {moving_count:,}")
        print(f"Distinct moving MMSI values after cleaning: {moving_vessel_count:,}")

        closest_encounters = find_close_encounters(moving_df)
        candidate_count = closest_encounters.count()
        output_path = output_dir / "closest_encounters"

        closest_encounters.coalesce(1).write.mode("overwrite").option("header", True).csv(
            str(output_path)
        )

        print(f"Close encounter candidates within threshold: {candidate_count:,}")
        print(f"Saved ranked candidates to: {output_path}")

        validated_event_df = select_validated_event(closest_encounters, moving_df)
        selected_event = validated_event_df.first()
        if selected_event is None:
            print("No close encounters passed trajectory-support validation.")
            return

        trajectory_df = extract_trajectory_window(moving_df, selected_event)
        trajectory_csv_dir, trajectory_plot_path = save_trajectory_outputs(
            trajectory_df,
            selected_event,
            output_dir,
        )

        print(format_event_summary(selected_event))
        print(
            "Trajectory support: "
            f"A={selected_event.points_a} points "
            f"({selected_event.points_before_a} before, {selected_event.points_after_a} after), "
            f"B={selected_event.points_b} points "
            f"({selected_event.points_before_b} before, {selected_event.points_after_b} after)"
        )
        print(f"Saved trajectory CSV to: {trajectory_csv_dir}")
        print(f"Saved trajectory plot to: {trajectory_plot_path}")
        print("Next step: inspect the validated plot and write the final report.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
