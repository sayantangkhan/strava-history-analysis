"""
Docstring for time_series_functions

This module contains functions that convert a time series into a scalar,
to be added to the main spine.
"""

import numpy as np
import polars as pl

from .activity_time_series import rolling_mean, rolling_normalized_power
from .time_series_parser import (
    MOVING_SPEED_THRESHOLD,
    get_time_series,
    source_columns,
    to_uniform_time_series,
)


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


## Adapters
#
# These predate get_activity_time_series and keep their original output for the scalar pipeline
# and the notebooks: a `duration` column, the requested fields with missing samples zero-filled,
# and `fIsMoving`, with no resampling. They raise ColumnNotFoundError when the activity didn't
# record a requested field, which the compute_* helpers turn into None. New code should use
# get_activity_time_series (or to_uniform_time_series) directly.
def general_adapter(
    fields: list[str],
    df: pl.DataFrame,
    moving_speed_threshold: float = MOVING_SPEED_THRESHOLD,
) -> pl.DataFrame:
    for f in fields:
        if not source_columns(df, f):
            raise pl.exceptions.ColumnNotFoundError(f"No {f} data in this time series")
    uniform = to_uniform_time_series(
        df, resample_1hz=False, moving_speed_threshold=moving_speed_threshold
    )
    return uniform.select(
        pl.col("elapsed").alias("duration"),
        *(pl.col(f).fill_null(0.0) for f in fields),
        pl.col("fIsMoving"),
    )


def general_power_adapter(
    df: pl.DataFrame, moving_speed_threshold=MOVING_SPEED_THRESHOLD
) -> pl.DataFrame:
    return general_adapter(["power"], df, moving_speed_threshold)


def general_hr_adapter(
    df: pl.DataFrame, moving_speed_threshold=MOVING_SPEED_THRESHOLD
) -> pl.DataFrame:
    return general_adapter(["heartrate"], df, moving_speed_threshold)


# Analytics


def normalized_power() -> list[pl.Expr]:
    thirty_second_average = pl.col("power").rolling_mean(30).alias("30s average")
    l4_norm = ((thirty_second_average**4).mean() ** 0.25).alias("Normalized power")
    return l4_norm


def peak_normalized_power(duration_seconds: int) -> pl.Expr:
    # See rolling_normalized_power for how the start of the ride is handled.
    return (
        rolling_normalized_power(duration_seconds).max().alias("Peak normalized power")
    )


def peak_average_power(duration_seconds: int) -> pl.Expr:
    return (
        rolling_mean("power", duration_seconds)
        .max()
        .alias(f"Peak {duration_seconds}s power")
    )


def average_power() -> pl.Expr:
    return pl.col("power").mean()


def peak_rolling_hr(duration_seconds: int) -> pl.Expr:
    return (
        rolling_mean("heartrate", duration_seconds)
        .max()
        .alias(f"Peak {duration_seconds}s HR")
    )
