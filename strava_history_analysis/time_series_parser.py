"""
Docstring for time_series_parser

In this module, we parse fit files and strava time series streams into a common time series format.
The common format will be a polars DataFrame.
"""

import json
import os
import tempfile
from datetime import datetime, timedelta

import fitparse
import polars as pl

# Listing out fields from the fit file I want to ignore for now
FIT_FILE_FIELDS_TO_IGNORE = {
    "left_right_balance (None)",
}


def parse_fit_file(fit_file_path: str) -> pl.DataFrame:
    """
    - We first iterate through the messages in the fit file
    - Filter to the ones of record type
    - Each record is a time snapshot of measured values
    - We iterate through the fields in that record, and populate the appropriate column in the dataframe
    """
    parsed_fit_file = fitparse.FitFile(fit_file_path)

    dataframe = {}
    first_row_fields = next(parsed_fit_file.get_messages("record")).fields
    for f in first_row_fields:
        f = f.as_dict()
        combined_name = f"{f['name']} ({f['units']})"
        dataframe[combined_name] = []

    for index, record in enumerate(parsed_fit_file.get_messages("record")):
        for f in record.fields:
            f = f.as_dict()
            combined_name = f"{f['name']} ({f['units']})"
            value = f["value"]

            # If this field is something we've not seen before, we populate it with None values until this point
            if combined_name not in dataframe:
                dataframe[combined_name] = [None] * index
            dataframe[combined_name].append(value)
        for v in dataframe.values():
            # Checking if some field was missing values in this record
            # If so, we populate with None, and hope polars knows how to deal with it
            if len(v) < index + 1:
                v.append(None)

    # Some of the fields are recorded at 2x the frequency, but with null values for half the recordings
    # we filter those out. It's unclear if it's the even or odd values recordings with the true values.
    true_length = len(dataframe["timestamp (None)"])
    for k, v in dataframe.items():
        if len(v) == 2 * true_length:
            v_prime = [r for (i, r) in enumerate(v) if i % 2 == 0]
            v_prime_prime = [r for (i, r) in enumerate(v) if i % 2 == 1]
            if all(r is None for r in v_prime):
                dataframe[k] = v_prime_prime
            elif all(r is None for r in v_prime_prime):
                dataframe[k] = v_prime
            else:
                raise ValueError("Both even and odd substreams had values")

    fields_to_remove = FIT_FILE_FIELDS_TO_IGNORE.intersection(dataframe.keys())
    for f in fields_to_remove:
        del dataframe[f]

    return pl.DataFrame(dataframe)


def parse_strava_series(series_file_path: str) -> pl.DataFrame:
    """
    We iterate through the fields specified in the json, with the different json keys corresponding to the different
    columns in the dataframe.
    """
    with open(series_file_path) as f:
        parsed_json_file = json.load(f)

    dataframe = {}
    for f in parsed_json_file:
        dataframe[f] = parsed_json_file[f]["data"]

    return pl.DataFrame(dataframe)


# ---- Uniform time series -----------------------------------------------------------------------
#
# The two source formats name, encode and time-stamp their fields differently. Everything below
# turns either one into a single frame with the columns in UNIFORM_SCHEMA, so analysis code never
# has to know which format an activity came from.

# Speed (m/s) at or above which a sample counts as moving. Applied to both formats: Strava's own
# `moving` flag in the API streams is ignored so that moving time means the same thing everywhere.
MOVING_SPEED_THRESHOLD = 1.5

# Uniform field name -> (fit columns in order of preference, strava API column).
# A fit file can carry both a plain and an "enhanced" version of a field; the first non-null wins.
FIELD_NAME_MAPPINGS = {
    "power": (["power (watts)"], "watts"),
    "heartrate": (["heart_rate (bpm)"], "heartrate"),
    "cadence": (["cadence (rpm)"], "cadence"),
    "speed": (["enhanced_speed (m/s)", "speed (m/s)"], "velocity_smooth"),
    "distance": (["distance (m)"], "distance"),
    "altitude": (["enhanced_altitude (m)", "altitude (m)"], "altitude"),
    "temperature": (["temperature (C)"], "temp"),
}

UNIFORM_SCHEMA = {
    "timestamp": pl.Datetime("us", "UTC"),
    "elapsed": pl.Duration("us"),
    "moving_time": pl.Duration("us"),
    "fIsMoving": pl.Boolean,
    **{field: pl.Float64 for field in FIELD_NAME_MAPPINGS},
    "lat": pl.Float64,
    "lng": pl.Float64,
}

_SEMICIRCLES_TO_DEGREES = 180 / 2**31


def is_strava_api_series(raw: pl.DataFrame) -> bool:
    """Whether a raw time series came from a Strava API stream pull (as opposed to a fit file)."""
    return "moving" in raw.columns


def source_columns(raw: pl.DataFrame, field: str) -> list[str]:
    """The raw columns that hold `field` (a key of FIELD_NAME_MAPPINGS) in this time series."""
    fit_names, strava_name = FIELD_NAME_MAPPINGS[field]
    candidates = [strava_name] if is_strava_api_series(raw) else fit_names
    return [c for c in candidates if c in raw.columns]


def _field_expr(raw: pl.DataFrame, field: str) -> pl.Expr:
    columns = source_columns(raw, field)
    if not columns:
        return pl.lit(None, dtype=pl.Float64).alias(field)
    return pl.coalesce([pl.col(c).cast(pl.Float64) for c in columns]).alias(field)


def to_uniform_time_series(
    raw: pl.DataFrame,
    start_time: datetime | None = None,
    resample_1hz: bool = True,
    moving_speed_threshold: float = MOVING_SPEED_THRESHOLD,
) -> pl.DataFrame:
    """
    Converts a raw time series (from either source format) into the columns of UNIFORM_SCHEMA.

    - `timestamp` is absolute UTC. Fit files carry it per sample; Strava API streams only carry
      seconds since the activity start, so `start_time` is needed (otherwise it is null).
    - `elapsed` is the time since the first sample; `moving_time` is the cumulative time spent in
      samples flagged `fIsMoving` (speed >= moving_speed_threshold).
    - Fields the activity didn't record are present but entirely null. Missing samples are left
      null too, never zero-filled: a zero heart rate or altitude is wrong, not missing.
    - With resample_1hz, the series has exactly one row per second of elapsed time, with recording
      gaps (auto-pause, dropouts) filled by null rows that are not moving, so row-count windows such
      as rolling_mean(n) span n seconds.
    """
    if is_strava_api_series(raw):
        offset = pl.duration(seconds=pl.col("time"))
        timestamp = (
            (pl.lit(start_time).dt.convert_time_zone("UTC") + offset)
            if start_time is not None
            else pl.lit(None, dtype=UNIFORM_SCHEMA["timestamp"])
        )
        elapsed = pl.duration(seconds=pl.col("time") - pl.col("time").first())
        if "latlng" in raw.columns:
            lat = pl.col("latlng").list.get(0, null_on_oob=True)
            lng = pl.col("latlng").list.get(1, null_on_oob=True)
        else:
            lat = lng = pl.lit(None, dtype=pl.Float64)
    else:
        # fitparse returns naive datetimes that are UTC per the fit spec
        fit_timestamp = pl.col("timestamp (None)").dt.replace_time_zone("UTC")
        timestamp = fit_timestamp
        elapsed = fit_timestamp - fit_timestamp.first()
        lat, lng = (
            pl.col(c).cast(pl.Float64) * _SEMICIRCLES_TO_DEGREES
            if c in raw.columns
            else pl.lit(None, dtype=pl.Float64)
            for c in ("position_lat (semicircles)", "position_long (semicircles)")
        )

    df = raw.select(
        timestamp.cast(UNIFORM_SCHEMA["timestamp"]).alias("timestamp"),
        elapsed.cast(UNIFORM_SCHEMA["elapsed"]).alias("elapsed"),
        *(_field_expr(raw, field) for field in FIELD_NAME_MAPPINGS),
        lat.cast(pl.Float64).alias("lat"),
        lng.cast(pl.Float64).alias("lng"),
    )

    if resample_1hz and df.height > 0:
        df = _resample_1hz(df)

    is_moving = (pl.col("speed") >= moving_speed_threshold).fill_null(False)
    step = pl.col("elapsed").diff().fill_null(timedelta(0))
    df = df.with_columns(is_moving.alias("fIsMoving")).with_columns(
        pl.when(pl.col("fIsMoving"))
        .then(step)
        .otherwise(timedelta(0))
        .cum_sum()
        .alias("moving_time")
    )

    return df.select(pl.col(name).cast(dtype) for name, dtype in UNIFORM_SCHEMA.items())


def _resample_1hz(df: pl.DataFrame) -> pl.DataFrame:
    """One row per whole second of elapsed time; duplicate samples keep the first, gaps are null."""
    seconds = pl.col("elapsed").dt.total_seconds()
    df = (
        df.with_columns(seconds.alias("_second"))
        .sort("_second")
        .unique(subset="_second", keep="first", maintain_order=True)
        .drop("elapsed")
    )
    grid = pl.DataFrame(
        {"_second": pl.int_range(0, df["_second"].max() + 1, eager=True)}
    )
    df = grid.join(df, on="_second", how="left").with_columns(
        pl.duration(seconds=pl.col("_second")).cast(pl.Duration("us")).alias("elapsed")
    )
    # Gap rows have no timestamp; recover it from the clock offset of the recorded rows.
    clock_start = (pl.col("timestamp") - pl.col("elapsed")).drop_nulls().first()
    return df.with_columns((clock_start + pl.col("elapsed")).alias("timestamp")).drop(
        "_second"
    )


def _spine_row(spine: pl.DataFrame, activity_id: int) -> dict:
    rows = spine.filter(pl.col("Activity ID") == activity_id)
    if rows.height != 1:
        raise KeyError(
            f"Expected one spine row for activity {activity_id}, found {rows.height}"
        )
    return rows.row(0, named=True)


def get_time_series_activity_id(
    spine: pl.DataFrame, activity_id: int, root_path: str = "./"
) -> pl.DataFrame:
    """The raw, format-specific time series for an activity. Prefer get_activity_time_series."""
    filename = _spine_row(spine, activity_id)["Filename"]
    return get_time_series(file_path=filename, root_path=root_path)


def get_activity_time_series(
    spine: pl.DataFrame,
    activity_id: int,
    root_path: str = "./",
    resample_1hz: bool = True,
    moving_speed_threshold: float = MOVING_SPEED_THRESHOLD,
) -> pl.DataFrame:
    """
    The time series for an activity in the uniform format (see to_uniform_time_series), whichever
    source it came from. This is the entry point analysis code should use.
    """
    row = _spine_row(spine, activity_id)
    raw = get_time_series(file_path=row["Filename"], root_path=root_path)
    return to_uniform_time_series(
        raw,
        start_time=row["Activity Date"],
        resample_1hz=resample_1hz,
        moving_speed_threshold=moving_speed_threshold,
    )


def get_time_series(file_path: str, root_path: str = "./") -> pl.DataFrame:
    """
    Returns the time series data for an activity, using a parquet cache to avoid re-parsing.

    :param file_path: Relative path to the source file (e.g., "fit_files/123.fit")
    :param root_path: Root directory of the project
    :return: Parsed time series as a DataFrame
    """
    full_source_path = os.path.join(root_path, file_path)

    # Derive cache path: fit_files/foo.fit -> cache/foo_fit.parquet
    #                    fit_files/foo.json -> cache/foo_json.parquet
    if file_path.endswith(".fit"):
        cache_relative = file_path.replace("fit_files/", "cache/").replace(
            ".fit", "_fit.parquet"
        )
    else:
        cache_relative = file_path.replace("fit_files/", "cache/").replace(
            ".json", "_json.parquet"
        )

    cache_path = os.path.join(root_path, cache_relative)

    if os.path.exists(cache_path):
        return pl.read_parquet(cache_path)

    # Parse based on file type
    if file_path.endswith(".fit"):
        df = parse_fit_file(full_source_path)
    else:
        df = parse_strava_series(full_source_path)

    cache_dir = os.path.dirname(cache_path)
    os.makedirs(cache_dir, exist_ok=True)
    # Write to a temporary file and rename it into place, so the cache only ever contains
    # complete files. Writing directly to cache_path leaves a truncated file behind if the
    # write is interrupted, and every later read then trusts that stub. The temp name is
    # unique so that two processes caching the same activity (a marimo kernel and a script,
    # say) don't rename each other's half-written file.
    fd, temp_path = tempfile.mkstemp(dir=cache_dir, suffix=".parquet.tmp")
    os.close(fd)
    try:
        df.write_parquet(temp_path)
        os.replace(temp_path, cache_path)
    except BaseException:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise
    return df
