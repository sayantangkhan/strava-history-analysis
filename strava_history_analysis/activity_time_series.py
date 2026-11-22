"""
Docstring for activity_time_series

This module contains functions that take the main time series associated to an activity, and
compute derived time series from it: one value per sample, such as rolling averages or rolling
normalized power. (time_series_functions, by contrast, reduces a time series to a scalar for the
spine; its peak_* reductions are the .max() of the series defined here.)

Everything here works on the uniform format from time_series_parser.get_activity_time_series.
Windows are given in seconds and implemented as row counts, which relies on that format's one row
per second. Filtering to moving samples (moving_only) keeps that property for moving time: each
remaining row is one moving second, so a window spans that much moving time, with stops cut out.

There are two layers:
- Expression builders (rolling_mean, rolling_normalized_power) return Polars expressions over the
  uniform columns, so they compose with each other and with scalar reductions.
- compute_* functions take a uniform time series DataFrame (filtered or trimmed however the caller
  likes) and return it with derived columns appended, via with_derived_series. Each has a
  compute_*_for_activity wrapper that runs it on an activity's whole series, looked up by ID.
"""

import polars as pl

from strava_history_analysis.time_series_parser import get_activity_time_series

# ---- Expression builders -----------------------------------------------------------------------


def rolling_mean(field: str, window_seconds: int) -> pl.Expr:
    """
    Trailing mean of `field` over `window_seconds`, named "<field>_<window>s". A window containing
    a null sample is null, and so are the first window_seconds - 1 rows.
    """
    return (
        pl.col(field).rolling_mean(window_seconds).alias(f"{field}_{window_seconds}s")
    )


def rolling_normalized_power(
    window_seconds: int, power: pl.Expr | None = None
) -> pl.Expr:
    """
    Trailing normalized power over `window_seconds`: the fourth-power mean of the 30s average power
    within the window, named "np_<window>s". `power` defaults to the "power" column.

    The 30s average is a rolling *sum* over 30, which treats the 29 rows before the ride has 30
    seconds of history as if power were zero — which it was, the rider wasn't pedalling yet. A
    plain rolling_mean leaves those rows null and the nulls then poison every outer window
    overlapping them, so a ride needed window_seconds + 29 samples before any value could be
    reported (a 20:27 TT returned None for its peak 20 minute power). Using
    rolling_mean(30, min_samples=1) instead would divide by the samples seen so far,
    under-smoothing a hard start: a ride opening with a 640W sprint reported a 210W peak 20 minute
    power against a true 185W. The outer window keeps the default min_samples, so the first
    window_seconds - 1 rows (and all of a ride shorter than the window) are null.
    """
    if power is None:
        power = pl.col("power")
    thirty_second_average = power.rolling_sum(30, min_samples=1) / 30
    return ((thirty_second_average**4).rolling_mean(window_seconds) ** 0.25).alias(
        f"np_{window_seconds}s"
    )


def power_with_dropouts_as_zero() -> pl.Expr:
    """
    The power column with missing samples (meter dropouts, recording gaps) counted as 0 W, as the
    scalar pipeline does. An activity with no power recorded at all stays entirely null, so derived
    series come out null rather than zero.
    """
    return (
        pl.when(pl.col("power").is_not_null().any())
        .then(pl.col("power").fill_null(0.0))
        .otherwise(None)
    )


# ---- Applying them -----------------------------------------------------------------------------


def with_derived_series(
    series: pl.DataFrame, *exprs: pl.Expr, moving_only: bool = True
) -> pl.DataFrame:
    """
    Appends derived columns to a uniform time series. With moving_only, non-moving samples are
    dropped first, so the derived series (and their windows) run over moving time.
    """
    if moving_only:
        series = series.filter(pl.col("fIsMoving"))
    return series.with_columns(*exprs)


def compute_rolling_np(
    series: pl.DataFrame, rolling_duration_seconds: int, moving_only: bool = True
) -> pl.DataFrame:
    """
    A uniform time series with a "rolling_np" column: normalized power over the trailing
    rolling_duration_seconds (of moving time, with moving_only). Missing power samples count as
    0 W; a series without power gets an all-null column.

    Trim the series before calling this, not after (e.g. cutting off the ride to the start of a
    brevet), so the windows don't reach into the trimmed part.
    """
    return with_derived_series(
        series,
        rolling_normalized_power(
            rolling_duration_seconds, power=power_with_dropouts_as_zero()
        ).alias("rolling_np"),
        moving_only=moving_only,
    )


# ---- By activity ID ----------------------------------------------------------------------------
#
# Thin wrappers that run a compute_* function above over an activity's whole, unfiltered uniform
# time series.


def compute_rolling_np_for_activity(
    activity_id: int,
    rolling_duration_seconds: int,
    spine: pl.DataFrame,
    moving_only: bool = True,
    root_path: str = "./",
) -> pl.DataFrame:
    """compute_rolling_np over the activity's whole time series."""
    series = get_activity_time_series(spine, activity_id, root_path)
    return compute_rolling_np(series, rolling_duration_seconds, moving_only=moving_only)
