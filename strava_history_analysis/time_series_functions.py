"""
Docstring for time_series_functions

This module contains functions that convert a time series into a scalar,
to be added to the main spine.
"""

import polars as pl
from typing import List
import numpy as np
from .time_series_parser import get_time_series


def compute_peak_normalized_power(duration_seconds, filename, root_path) -> np.float64:
    f = peak_normalized_power(duration_seconds)
    return compute_power_functional(f, filename, root_path)


def compute_peak_average_power(duration_seconds, filename, root_path) -> np.float64:
    f = peak_average_power(duration_seconds)
    return compute_power_functional(f, filename, root_path)


def compute_average_power(filename, root_path) -> np.float64:
    return compute_power_functional(average_power(), filename, root_path)


def compute_normalized_power(filename, root_path) -> np.float64:
    return compute_power_functional(normalized_power(), filename, root_path)


def compute_power_functional(functional, filename, root_path) -> np.float64:
    try:
        ts_df = general_power_adapter(
            get_time_series(file_path=filename, root_path=root_path)
        )

        res = ts_df.select(functional)
        colname = res.columns[0]

        return (res)[colname][0]
    except pl.exceptions.ColumnNotFoundError:
        return None


# This is a dictionary where the keys are the names of
# the fields we will use in our code, and the values are 2-tuples,
# the first element of which is what the field is called in the garmin fit
# and the second element is what it's called strava data.
FIELD_NAME_MAPPINGS = {
    "power": ("power (watts)", "watts"),
    "heartrate": ("heart_rate (bpm)", "heartrate"),
}


## The various field adapters go in here
def fit_adapter(
    fields: List[str], df: pl.DataFrame, moving_speed_threshold: float = 1.5
):
    speed = df.get_column(
        "speed (m/s)", default=pl.repeat(0.0, df.shape[0], dtype=pl.Float64)
    )
    enhanced_speed = df.get_column(
        "enhanced_speed (m/s)", default=pl.repeat(0.0, df.shape[0], dtype=pl.Float64)
    )

    fIsMoving = (speed >= moving_speed_threshold) | (
        enhanced_speed >= moving_speed_threshold
    )

    selectors = []
    selectors.append(
        (pl.col("timestamp (None)") - pl.col("timestamp (None)").first()).alias(
            "duration"
        )
    )
    for f in fields:
        selectors.append(
            pl.col(FIELD_NAME_MAPPINGS[f][0])
            .cast(pl.Float64)
            .fill_null(strategy="zero")
            .alias(f)
        )

    selectors.append((fIsMoving).alias("fIsMoving"))

    return df.select(selectors)


def strava_api_adapter(fields: List[str], df: pl.DataFrame):
    selectors = []
    selectors.append(
        (pl.duration(seconds=(pl.col("time") - pl.col("time").first()))).alias(
            "duration"
        )
    )

    for f in fields:
        selectors.append(
            pl.col(FIELD_NAME_MAPPINGS[f][1])
            .cast(pl.Float64)
            .fill_null(strategy="zero")
            .alias(f)
        )

    selectors.append(
        (pl.col("moving")).alias("fIsMoving"),
    )

    return df.select(selectors)


# Power
def fit_power_adapter(df: pl.DataFrame, moving_speed_threshold=1.5) -> pl.DataFrame:
    return fit_adapter(
        ["power"],
        df,
        moving_speed_threshold,
    )


def strava_api_power_adapter(df: pl.DataFrame) -> pl.DataFrame:
    return strava_api_adapter(
        ["power"],
        df,
    )


# HR
def fit_hr_adapter(df: pl.DataFrame, moving_speed_threshold=1.5) -> pl.DataFrame:
    return fit_adapter(
        ["heartrate"],
        df,
        moving_speed_threshold,
    )


def strava_api_hr_adapter(df: pl.DataFrame) -> pl.DataFrame:
    return strava_api_adapter(
        ["heartrate"],
        df,
    )


def general_power_adapter(df: pl.DataFrame, moving_speed_threshold=1.5) -> pl.DataFrame:
    # Uses one of the two power adapter functions based on whether the dataframe
    # comes from a fit file or a strava api pull
    if "moving" in df:
        # Comes from strava api pull
        return strava_api_power_adapter(df)
    else:
        return fit_power_adapter(df, moving_speed_threshold=moving_speed_threshold)


def general_hr_adapter(df: pl.DataFrame, moving_speed_threshold=1.5) -> pl.DataFrame:
    # Uses one of the two power adapter functions based on whether the dataframe
    # comes from a fit file or a strava api pull
    if "moving" in df:
        # Comes from strava api pull
        return strava_api_hr_adapter(df)
    else:
        return fit_hr_adapter(df, moving_speed_threshold=moving_speed_threshold)


# TODO: Make this more general
def fill_duration_gaps(df: pl.DataFrame) -> pl.DataFrame:
    """
    Takes a dataframe with duration, power, and fIsMoving columns and returns
    a dataframe with evenly spaced 1-second duration rows, filling gaps where
    the original data had increments > 1s.
    """
    duration_seconds = df.select(
        pl.col("duration").dt.total_seconds().cast(pl.Int64).alias("seconds")
    ).get_column("seconds")

    max_seconds = duration_seconds.max()

    complete_range = pl.DataFrame(
        {
            "duration": (pl.Series(range(0, max_seconds + 1)) * 1e6).cast(
                pl.Duration("us")
            )
        }
    )

    result = complete_range.join(df, on="duration", how="left")

    result = result.with_columns(
        [
            pl.col("power").fill_null(0.0),
            pl.col("fIsMoving").fill_null(False),
        ]
    )

    return result


# Analytics


def normalized_power() -> List[pl.Expr]:
    thirty_second_average = pl.col("power").rolling_mean(30).alias("30s average")
    l4_norm = ((thirty_second_average**4).mean() ** 0.25).alias("Normalized power")
    return l4_norm


def peak_normalized_power(duration_seconds: int) -> pl.Expr:
    thirty_second_average = pl.col("power").rolling_mean(30).alias("30s average")
    l4_norm = ((thirty_second_average**4).rolling_mean(duration_seconds) ** 0.25).alias(
        "Normalized power"
    )
    peak_np = l4_norm.max().alias("Peak normalized power")
    return peak_np


def peak_average_power(duration_seconds: int) -> pl.Expr:
    n_second_average = pl.col("power").rolling_mean(duration_seconds)
    return n_second_average.max().alias(f"Peak {duration_seconds}s power")


def average_power() -> pl.Expr:
    return pl.col("power").mean()


def peak_rolling_hr(duration_seconds: int) -> pl.Expr:
    n_second_average = pl.col("heartrate").rolling_mean(duration_seconds)
    return n_second_average.max().alias(f"Peak {duration_seconds}s HR")
