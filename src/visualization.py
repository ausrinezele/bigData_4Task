from datetime import timedelta
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
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

    plot_trajectory_pdf(pdf, event, plot_path)

    return csv_dir, plot_path


def plot_trajectory_pdf(pdf, event: Row, plot_path: Path) -> None:
    pdf = pdf.copy()
    pdf["timestamp"] = pdf["timestamp"].astype(str)
    pdf["vessel_label"] = pdf.apply(
        lambda row: f"{row['mmsi']} - {row['vessel_name']}"
        if row.get("vessel_name")
        else str(row["mmsi"]),
        axis=1,
    )

    fig, ax = plt.subplots(figsize=(11, 8.5))
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for index, (label, vessel_points) in enumerate(pdf.groupby("vessel_label")):
        vessel_points = vessel_points.sort_values("timestamp")
        color = colors[index % len(colors)]
        ax.plot(
            vessel_points["longitude"],
            vessel_points["latitude"],
            marker="o",
            markersize=3.2,
            linewidth=1.8,
            color=color,
            label=label,
        )

        if len(vessel_points) >= 2:
            arrow_start = vessel_points.iloc[-2]
            arrow_end = vessel_points.iloc[-1]
            ax.annotate(
                "",
                xy=(arrow_end["longitude"], arrow_end["latitude"]),
                xytext=(arrow_start["longitude"], arrow_start["latitude"]),
                arrowprops={"arrowstyle": "->", "lw": 1.8, "color": color},
            )

        first = vessel_points.iloc[0]
        last = vessel_points.iloc[-1]
        ax.scatter([first["longitude"]], [first["latitude"]], color=color, s=42, marker="s")
        ax.scatter([last["longitude"]], [last["latitude"]], color=color, s=52, marker="^")
        ax.annotate(
            "start",
            (first["longitude"], first["latitude"]),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=8,
            color=color,
        )
        ax.annotate(
            "end",
            (last["longitude"], last["latitude"]),
            xytext=(5, -10),
            textcoords="offset points",
            fontsize=8,
            color=color,
        )

    ax.scatter(
        [event.event_longitude],
        [event.event_latitude],
        color="red",
        marker="x",
        s=180,
        linewidths=3,
        label="closest point",
        zorder=5,
    )
    ax.annotate(
        f"{event.distance_meters:.2f} m",
        (event.event_longitude, event.event_latitude),
        xytext=(12, 12),
        textcoords="offset points",
        fontsize=9,
        color="red",
        weight="bold",
    )

    event_timestamp_text = str(event.event_timestamp)
    if "UTC" not in event_timestamp_text:
        event_timestamp_text = f"{event_timestamp_text} UTC"

    ax.set_title(
        "Validated Closest Vessel Encounter\n"
        f"{event_timestamp_text} | "
        f"{event.distance_meters:.2f} m separation | "
        f"{event.time_delta_seconds} s timestamp gap"
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    mean_latitude = pdf["latitude"].mean()
    ax.set_aspect(1.0 / max(0.1, math.cos(math.radians(mean_latitude))))
    ax.ticklabel_format(axis="both", style="plain", useOffset=False)
    ax.xaxis.set_major_formatter(FormatStrFormatter("%.6f"))
    ax.yaxis.set_major_formatter(FormatStrFormatter("%.6f"))
    ax.margins(0.08)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", framealpha=0.95)
    fig.tight_layout()
    fig.savefig(plot_path, dpi=200)
    plt.close(fig)


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
