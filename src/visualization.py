from datetime import timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F


def extract_trajectory_window(moving_df: DataFrame, event: Row, minutes: int = 10) -> DataFrame:
    start_time = event.event_timestamp - timedelta(minutes=minutes)
    end_time = event.event_timestamp + timedelta(minutes=minutes)

    return (
        moving_df.filter(F.col("mmsi").isin([event.mmsi_a, event.mmsi_b]))
        .filter((F.col("timestamp") >= F.lit(start_time)) & (F.col("timestamp") <= F.lit(end_time)))
        .select("mmsi", "vessel_name", "timestamp", "latitude", "longitude", "sog", "cog")
        .orderBy("mmsi", "timestamp")
    )


def save_trajectory_outputs(
    trajectory_df: DataFrame,
    event: Row,
    output_dir: Path,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_dir = output_dir / "collision_trajectory_csv"
    plot_path = output_dir / "collision_trajectory.png"

    trajectory_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv_dir))

    pdf = trajectory_df.toPandas()
    if pdf.empty:
        raise RuntimeError("No trajectory records found for the selected event window.")

    pdf["timestamp"] = pdf["timestamp"].astype(str)
    pdf["vessel_label"] = pdf.apply(
        lambda row: f"{row['mmsi']} - {row['vessel_name']}"
        if row.get("vessel_name")
        else str(row["mmsi"]),
        axis=1,
    )

    fig, ax = plt.subplots(figsize=(10, 8))

    for label, vessel_points in pdf.groupby("vessel_label"):
        vessel_points = vessel_points.sort_values("timestamp")
        ax.plot(
            vessel_points["longitude"],
            vessel_points["latitude"],
            marker="o",
            markersize=3,
            linewidth=1.5,
            label=label,
        )
        first = vessel_points.iloc[0]
        last = vessel_points.iloc[-1]
        ax.annotate("start", (first["longitude"], first["latitude"]), fontsize=8)
        ax.annotate("end", (last["longitude"], last["latitude"]), fontsize=8)

    ax.scatter(
        [event.event_longitude],
        [event.event_latitude],
        color="red",
        marker="x",
        s=120,
        linewidths=2,
        label="closest point",
    )

    ax.set_title(
        "Closest Vessel Encounter Trajectories\n"
        f"{event.event_timestamp} UTC, distance {event.distance_meters:.2f} m"
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(plot_path, dpi=200)
    plt.close(fig)

    return csv_dir, plot_path


def format_event_summary(event: Row) -> str:
    name_a = event.vessel_name_a or "UNKNOWN"
    name_b = event.vessel_name_b or "UNKNOWN"

    return "\n".join(
        [
            "Selected closest encounter:",
            f"  Vessel A: MMSI {event.mmsi_a}, name {name_a}",
            f"  Vessel B: MMSI {event.mmsi_b}, name {name_b}",
            f"  Timestamp: {event.event_timestamp} UTC",
            f"  Coordinates: {event.event_latitude:.6f}, {event.event_longitude:.6f}",
            f"  Distance: {event.distance_meters:.2f} meters",
            f"  Time delta: {event.time_delta_seconds} seconds",
        ]
    )
