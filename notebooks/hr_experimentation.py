import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full", app_title="Rolling peak HR")


@app.cell
def _():
    import marimo as mo  # noqa: F401

    import polars as pl
    import matplotlib.pyplot as plt
    import numpy as np

    from strava_history_analysis import get_spine, get_time_series
    from strava_history_analysis.time_series_functions import (
        general_hr_adapter,
        peak_rolling_hr,
    )

    return (
        general_hr_adapter,
        get_spine,
        get_time_series,
        mo,
        peak_rolling_hr,
        pl,
        plt,
    )


@app.cell
def _(mo):
    mo.md("""
    # Outline of the historical normalized power notebook

    1. We load the full dataset
    2. We then define the various HR related metrics we want to compute for each activity
    3. Then we augment the `df` dataframe by adding those columns via a `map_elements`.
    4. And then we filter to the set we care about, and plot the data.
    """)
    return


@app.cell
def _(get_spine):
    df = get_spine(root_path="../", poll_strava=True)
    return (df,)


@app.cell
def _(general_hr_adapter, get_time_series, peak_rolling_hr, pl):
    def compute_peak_rolling_hr(filename, root_path) -> "np.float64":
        try:
            ts_df = general_hr_adapter(
                get_time_series(file_path=filename, root_path=root_path)
            )
            return (ts_df.select(peak_rolling_hr(60)))["Peak 60s HR"][0]
        except pl.exceptions.ColumnNotFoundError:
            return None  # ty:ignore[invalid-return-type]

    return (compute_peak_rolling_hr,)


@app.cell
def _(compute_peak_rolling_hr, df, pl):
    dfnp = df.with_columns(
        [
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_rolling_hr(f, "../"),
                return_dtype=pl.Float64,
            )
            .alias("Peak 60s HR"),
        ]
    )
    return (dfnp,)


@app.cell
def _(dfnp, pl):
    dfnp.select(pl.col("Activity Name"), pl.col("Peak 60s HR")).sample(10)
    return


@app.cell
def _(dfnp, pl, plt):
    f_valid = (pl.col("Elapsed Time") / 3600 >= 0.9) & (pl.col("Peak 60s HR") >= 100)

    plt.figure(figsize=(20, 10))

    plt.scatter(
        dfnp.filter(f_valid)["Activity Date"],
        dfnp.filter(f_valid)["Peak 60s HR"],
        s=(dfnp.filter(f_valid)["Elapsed Time"] / 3000) ** 1.5 * 5,
        alpha=0.6,
    )

    plt.legend(loc=2)

    plt.xlabel("Date")
    plt.ylabel("Peak 60 HR")

    plt.grid()

    plt.gca()
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
