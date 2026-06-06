# AIS Collision Detector

This project processes Danish AIS data with PySpark to identify the closest
validated vessel proximity event inside the required marine area for December
2021. The result is reported as a collision-indicating AIS proximity event,
because AIS records alone cannot prove an externally confirmed maritime
accident.

## Assignment Scope

- Data source: Danish AIS data
- Timeframe: `2021-12-01 00:00:00` to `2022-01-01 00:00:00`
- Area center: latitude `55.225000`, longitude `14.245000`
- Area radius: `50 nautical miles` / `92.6 km`
- Processing framework: Apache Spark via PySpark
- Containerization: Docker

## Final Result

The selected validated closest-proximity event is:

| Field | Value |
| --- | --- |
| Vessel A | `265547110`, `RESCUE SJOMANSHUSET` |
| Vessel B | `265695200`, `RESCUE PANTAMERA` |
| Timestamp | `2021-12-16 10:25:03 UTC` |
| Coordinates | `55.575245`, `14.356953` |
| Minimum separation | `0.19 meters` |
| Timestamp gap | `4 seconds` |

See [REPORT.md](REPORT.md) for methodology, validation and
computational details.

## Repository Structure

```text
src/
  ais_loader.py            Spark CSV loading and timestamp parsing
  cleaning.py              Duplicate, stationary-vessel, and GPS-noise filtering
  collision_detection.py   Spatiotemporal bucket join and distance ranking
  config.py                Assignment constants and thresholds
  main.py                  Pipeline entrypoint
  spatial_filter.py        Coordinate validation and radius filtering
  validation.py            Candidate trajectory-support validation
  visualization.py         Trajectory output and plotting

data/                      Local AIS input files, ignored by git
outputs/                   Generated results and plots, ignored by git
Dockerfile                 Container definition
requirements.txt           Python dependencies
REPORT.md                  Written report
```

## Data Setup

Download and extract the December 2021 Danish AIS CSV files manually, then place
the extracted CSV files somewhere under:

```text
data/
```

The loader searches recursively using:

```text
data/**/*.csv
```

The `data/` directory is ignored by git because the AIS archive is too large to
commit.

## Build Docker Image

From the repository root:

```bash
docker build -t ais-collision-detector .
```

## Run Pipeline

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/outputs:/app/outputs" \
  ais-collision-detector
```

The run prints row counts, candidate counts, and the selected event summary.

## Outputs

After a successful run, the main generated outputs are:

```text
outputs/closest_encounters/
outputs/collision_trajectory_csv/
outputs/collision_trajectory.png
outputs/final_result.txt
```

- `closest_encounters/` contains the ranked candidate proximity events.
- `collision_trajectory_csv/` contains the 20-minute trajectory window for the
  selected vessel pair.
- `collision_trajectory.png` visualizes both vessel tracks from 10 minutes
  before to 10 minutes after the closest point.
- `final_result.txt` contains a compact commit-friendly summary of the final
  selected event.

## Method Summary

The pipeline:

1. Loads raw AIS CSV files with PySpark.
2. Filters records to December 2021.
3. Removes invalid coordinates and applies the 50 nautical mile radius filter.
4. Removes duplicates, stationary vessels, and unrealistic GPS jumps.
5. Uses one-minute time buckets and neighboring 100-meter spatial grid cells to
   avoid a full Cartesian product.
6. Calculates exact haversine distances only for candidate pairs.
7. Validates the selected event by requiring trajectory support before and after
   the closest point for both vessels.
8. Saves the trajectory plot and candidate outputs.

Pandas is used only for plotting the small final trajectory window after Spark
has already completed the large-scale processing.

## Docker Image Export

Docker Hub image link:

```text
https://hub.docker.com/r/ausrbu/ais-collision-detector
```

