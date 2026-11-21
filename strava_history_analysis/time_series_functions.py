"""
Docstring for time_series_functions

This module contains functions that convert a time series into a scalar,
to be added to the main spine.
"""

import polars as pl
from typing import List
import numpy as np
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
    # The 30s average is a rolling *sum* over 30, which treats the 29 rows before the ride
    # has 30 seconds of history as if power were zero — which it was, the rider wasn't
    # pedalling yet. A plain rolling_mean leaves those rows null and the nulls then poison
    # every outer window overlapping them, so a ride needed duration_seconds + 29 samples
    # before any peak could be reported (a 20:27 TT returned None for its peak 20 minute
    # power). Using rolling_mean(30, min_samples=1) instead would divide by the samples
    # seen so far, under-smoothing a hard start: a ride opening with a 640W sprint reported
    # a 210W peak 20 minute power against a true 185W. The outer window keeps the default
    # min_samples, so a ride shorter than duration_seconds still yields null.
    thirty_second_average = (pl.col("power").rolling_sum(30, min_samples=1) / 30).alias(
        "30s average"
    )
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
