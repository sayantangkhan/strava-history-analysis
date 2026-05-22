import marimo

__generated_with = "0.23.7"
app = marimo.App(width="full", app_title="Historical Normalized Power")


@app.cell
def _():
    import marimo as mo

    import polars as pl
    import matplotlib.pyplot as plt
    import numpy as np

    from strava_history_analysis import get_spine, get_time_series
    from strava_history_analysis.time_series_functions import (
        general_power_adapter,
        normalized_power,
        peak_normalized_power,
    )

    return (
        general_power_adapter,
        get_spine,
        get_time_series,
        mo,
        normalized_power,
        np,
        peak_normalized_power,
        pl,
        plt,
    )


@app.cell
def _(mo):
    mo.md("""
    # Outline of the historical normalized power notebook

    1. We load the full dataset
    2. We then define the various power related metrics we want to compute for each activity
    3. Then we augment the `df` dataframe by adding those columns via a `map_elements`.
    4. And then we filter to the set we care about, and plot the data.
    """)
    return


@app.cell
def _(get_spine):
    df = get_spine(root_path="./", poll_strava=True)
    return (df,)


@app.cell
def _(
    general_power_adapter,
    get_time_series,
    normalized_power,
    np,
    peak_normalized_power,
    pl,
):
    def compute_normalized_power(filename, root_path) -> np.float64:
        try:
            ts_df = general_power_adapter(
                get_time_series(file_path=filename, root_path=root_path)
            )
            return (ts_df.filter(pl.col("fIsMoving")).select(normalized_power()))[
                "Normalized power"
            ][0]
        except pl.exceptions.ColumnNotFoundError:
            return None


    def compute_peak_normalized_power(
        duration_seconds, filename, root_path
    ) -> np.float64:
        try:
            ts_df = general_power_adapter(
                get_time_series(file_path=filename, root_path=root_path)
            )
            return (ts_df.select(peak_normalized_power(duration_seconds)))[
                "Peak normalized power"
            ][0]
        except pl.exceptions.ColumnNotFoundError:
            return None

    return compute_normalized_power, compute_peak_normalized_power


@app.cell
def _(df):
    df.tail(10)
    return


@app.cell
def _(compute_normalized_power, compute_peak_normalized_power, df, pl):
    root_path = "./"

    dfnp = df.with_columns(
        [
            pl.col("Filename")
            .map_elements(
                lambda f: compute_normalized_power(f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Normalized power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_normalized_power(3600, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 1h normalized power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_normalized_power(7200, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 2h normalized power"),
        ]
    )
    return (dfnp,)


@app.cell
def _(dfnp):
    dfnp.tail(10)
    return


@app.cell
def _(pl):
    f_valid = (
        (pl.col("Normalized power") >= 50)
        & (pl.col("Peak 1h normalized power").is_finite())
        # & (pl.col("Peak 1h normalized power") > 220)
        & (pl.col("Distance") >= 30)
        & (pl.col("Activity Type") == "Ride")
    )

    f_real_gear = pl.col("Activity Gear").is_not_null()
    return f_real_gear, f_valid


@app.cell
def _(dfnp, f_real_gear, f_valid):
    bikes = (
        dfnp.filter(f_valid & f_real_gear)["Activity Gear"]
        .value_counts()
        .sort("count", descending=True)
    )
    bikes
    return (bikes,)


@app.cell
def _(bikes, dfnp, f_valid, pl, plt):
    filters = []
    for b in bikes["Activity Gear"]:
        f = pl.col("Activity Gear") == b
        filters.append((b, f))

    markers = [
        "o",
        "^",
        "P",
        "*",
        "x",
        "d",
        # "s",
    ]

    plt.figure(figsize=(20, 10))

    for (b, f), m in zip(filters, markers):
        plt.scatter(
            dfnp.filter(f_valid & f)["Activity Date"],
            dfnp.filter(f_valid & f)["Peak 1h normalized power"],
            s=(dfnp.filter(f_valid & f)["Distance"] / 20) ** 1.5 * 5,
            marker=m,
            label=b,
            alpha=0.6,
        )

    plt.ylim((150, 260))
    plt.legend(loc=2)

    plt.xlabel("Date")
    plt.ylabel("Ride peak 1h normalized power")

    plt.grid()

    plt.gca()
    return


@app.cell
def _(mo):
    mo.md("""
    ## Looking for examples with really high peak 1h NP.
    """)
    return


@app.cell
def _(f_valid, pl):
    f_high_peak = (pl.col("Peak 1h normalized power") > 230) & (f_valid)
    return (f_high_peak,)


@app.cell
def _(dfnp, f_high_peak):
    dfnp.filter(f_high_peak).tail(10)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
