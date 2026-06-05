# Report

## Objective

The objective was to process Danish AIS data for December 2021 and identify the
two vessels with the closest collision-like physical proximity inside the
assignment area: a 50 nautical mile radius around latitude `55.225000`,
longitude `14.245000`.

The final result is reported as a validated AIS proximity event rather than an
externally confirmed maritime accident. AIS records can show that two reported
positions were extremely close in time and space, but AIS alone cannot prove
physical contact.

## Data Scope

The analysis uses manually downloaded and extracted Danish AIS CSV files from
the public Danish AIS data source. Raw records are processed with PySpark inside
Docker.

- Timeframe: `2021-12-01 00:00:00` to `2022-01-01 00:00:00`
- Center coordinate: `55.225000`, `14.245000`
- Radius: `50 nautical miles`, equivalent to `92.6 km`
- Input location: `data/**/*.csv`
- Main outputs:
  - `outputs/closest_encounters/`
  - `outputs/collision_trajectory_csv/`
  - `outputs/collision_trajectory.png`

## Processing Summary

The pipeline first loads the raw AIS CSV files with Spark and normalizes the
column names used by the Danish AIS files. The relevant fields are timestamp,
MMSI, latitude, longitude, speed over ground, course over ground, navigational
status, vessel name, and ship type.

Observed row counts from the completed run:

| Stage | Rows | Distinct MMSI |
| --- | ---: | ---: |
| December 2021 records | 318,325,485 | 8,667 |
| Valid coordinates | 313,370,297 | - |
| Inside 50 nautical mile area | 27,585,597 | 2,424 |
| Moving, cleaned records | 18,881,300 | 2,332 |

## Spatial Filtering

Spatial filtering is applied early to reduce the data volume before any
pairwise vessel comparison. The distance from each AIS record to the assignment
center is calculated with the haversine formula using an Earth radius of
`6371.0088 km`. Records are kept only if their distance to the center is less
than or equal to `92.6 km`.

This filtering step reduced the working dataset from more than 313 million
valid-coordinate records to about 27.6 million records inside the required
marine area.

## Data Cleaning

AIS data often contains duplicated records, stale positions, invalid
coordinates, and GPS jumps. The following cleaning rules were applied before
collision search:

- Remove records with null MMSI, timestamp, latitude, or longitude.
- Remove coordinates outside valid latitude and longitude ranges.
- Drop duplicate positions using `mmsi`, `timestamp`, `latitude`, and
  `longitude`.
- Use Spark window functions partitioned by MMSI to compare each vessel point
  with its previous point.
- Calculate distance from the previous point and implied speed.
- Remove GPS jumps where implied speed exceeds `80 knots`.
- Treat vessels as moving if `SOG >= 0.5 knots` or if the vessel moved at least
  `0.05 km` from the previous point within a maximum gap of 30 minutes.
- Exclude records whose navigational status indicates stationary behavior, such
  as anchored, moored, aground, or undefined.

These rules are designed to prevent false positives where two stationary vessels
are near each other in a harbor or where a single bad GPS point creates an
impossible apparent encounter.

## Collision Search Strategy

A full Cartesian product of all moving AIS points would be computationally
impractical. Instead, the pipeline uses spatiotemporal bucketing:

- Time bucket size: `60 seconds`
- Time tolerance after join: `60 seconds`
- Spatial grid size: `100 meters`
- Neighboring spatial cells are included so candidates on grid boundaries are
  not missed.
- Candidate pairs are restricted to different MMSI values, using `mmsi_a <
  mmsi_b` to avoid duplicate pair comparisons.
- Exact haversine distance is calculated only after the bucket join.
- Candidate events are retained when exact separation is at most `100 meters`.

This approach avoids an unbounded Cartesian join and limits exact distance
calculations to records that are already close in both time and approximate
space.

## Candidate Validation

The initial ranked candidate list included AIS artifacts where one vessel had
only a single point exactly matching another vessel's continuous trajectory.
Those were rejected as likely identity or GPS duplication artifacts.

To make the selected event more reliable, the final selector requires both
vessels to have trajectory support around the encounter:

- At least 5 AIS points in the 20-minute event window.
- At least 2 points before the closest point.
- At least 2 points after the closest point.

This validation step ensures the final event is supported by continuous
movement from both vessels rather than by a single isolated AIS record.

## Final Result

The closest validated AIS proximity event was:

| Field | Value |
| --- | --- |
| Vessel A MMSI | `265547110` |
| Vessel A name | `RESCUE SJOMANSHUSET` |
| Vessel B MMSI | `265695200` |
| Vessel B name | `RESCUE PANTAMERA` |
| Timestamp | `2021-12-16 10:25:03 UTC` |
| Latitude | `55.575245` |
| Longitude | `14.356953` |
| Minimum separation | `0.19 meters` |
| Timestamp difference | `4 seconds` |

Trajectory support:

- Vessel A: 70 points in the 20-minute window, with 38 before and 31 after.
- Vessel B: 69 points in the 20-minute window, with 37 before and 32 after.

The vessels were moving slowly and operating in very close proximity. Because
both vessels are rescue vessels and continue to report positions before and
after the event, this should be interpreted as the closest validated
collision-indicating AIS proximity event, not as an independently confirmed
accident.

## Visualization

The generated visualization is saved as:

```text
outputs/collision_trajectory.png
```

It shows both vessel trajectories from 10 minutes before to 10 minutes after the
selected event. The closest point is marked in red, and the plotted tracks
include start/end markers and direction indicators.

## Computational Notes

All raw data loading, filtering, cleaning, and candidate detection are performed
with PySpark. Pandas is used only after Spark extracts the small 20-minute
trajectory window for plotting. Large intermediate datasets are persisted using
disk-backed Spark storage to reduce memory pressure inside Docker.