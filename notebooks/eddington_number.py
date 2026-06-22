import marimo

__generated_with = "0.23.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import polars as pl
    import matplotlib.pyplot as plt
    import numpy as np

    import polars.selectors as cs
    from datetime import datetime, timezone

    from strava_history_analysis import get_spine

    return get_spine, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Eddington number

    The km/mile Eddington number is the largest integer $E$ such that there are $E$ rides longer than $E$ km/miles.
    """)
    return


@app.cell
def _(get_spine):
    df = get_spine(root_path="./", poll_strava=True)
    return (df,)


@app.cell
def _(df, pl):
    df_ride = df.filter(
        (pl.col("Activity Type") == 'Ride')
    ).select(
        [
            pl.col("Activity ID"),
            pl.col("Activity Date"),
            pl.col("Distance").alias("Distance (km)"),
            (pl.col("Distance") / 1.61).alias("Distance (mile)"),
            (pl.col("Moving Time") / 3600).alias("Hours"),
        ]
    )
    return (df_ride,)


@app.cell
def _(pl):
    def compute_eddington_number(distance_series: pl.Series) -> int:
        sorted_series = sorted(distance_series, key=lambda x: -x)
        estimate = 0
        while estimate < sorted_series[estimate]:
            estimate += 1
        return estimate

    return (compute_eddington_number,)


@app.cell
def _(compute_eddington_number, df_ride):
    compute_eddington_number(df_ride["Distance (km)"])
    return


@app.cell
def _(compute_eddington_number, df_ride):
    compute_eddington_number(df_ride["Distance (mile)"])
    return


@app.cell
def _(compute_eddington_number, df_ride):
    compute_eddington_number(df_ride["Hours"])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
