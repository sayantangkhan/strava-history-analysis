"""
Docstring for time_series_functions

This module contains functions that convert a time series into a scalar,
to be added to the main spine.
"""

import polars as pl
from typing import List


## The various field adapters go in here


# Power
def fit_power_adapter(df: pl.DataFrame, moving_speed_threshold=1.5) -> pl.DataFrame:
    speed = df.get_column(
        "speed (m/s)", default=pl.repeat(0.0, df.shape[0], dtype=pl.Float64)
    )
    enhanced_speed = df.get_column(
        "enhanced_speed (m/s)", default=pl.repeat(0.0, df.shape[0], dtype=pl.Float64)
    )

    fIsMoving = (speed >= moving_speed_threshold) | (
        enhanced_speed >= moving_speed_threshold
    )

    return df.select(
        [
            (pl.col("timestamp (None)") - pl.col("timestamp (None)").first()).alias(
                "duration"
            ),
            pl.col("power (watts)").fill_null(strategy="zero").alias("power"),
            (fIsMoving).alias("fIsMoving"),
        ]
    )


def strava_api_power_adapter(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        [
            (pl.duration(seconds=(pl.col("time") - pl.col("time").first()))).alias(
                "duration"
            ),
            (pl.col("watts").cast(pl.Float64).fill_null(strategy="zero")).alias(
                "power"
            ),
            (pl.col("moving")).alias("fIsMoving"),
        ]
    )


def general_power_adapter(df: pl.DataFrame, moving_speed_threshold=1.5) -> pl.DataFrame:
    # Uses one of the two power adapter functions based on whether the dataframe
    # comes from a fit file or a strava api pull
    if "moving" in df:
        # Comes from strava api pull
        return strava_api_power_adapter(df)
    else:
        return fit_power_adapter(df, moving_speed_threshold=moving_speed_threshold)


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
    return [
        thirty_second_average,
        l4_norm,
    ]


def peak_normalized_power(duration_seconds: int) -> pl.Expr:
    thirty_second_average = pl.col("power").rolling_mean(30).alias("30s average")
    l4_norm = (
        (thirty_second_average**4).rolling_mean(duration_seconds) ** 0.25
    ).alias("Normalized power")
    peak_np = l4_norm.max().alias("Peak normalized power")
    return peak_np
