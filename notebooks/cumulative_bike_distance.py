import marimo

__generated_with = "0.23.7"
app = marimo.App(width="medium", app_title="Cumulative bike distance")


@app.cell
def _():
    import marimo as mo

    import polars as pl
    import matplotlib.pyplot as plt
    import numpy as np

    import polars.selectors as cs
    from datetime import datetime, timezone

    from strava_history_analysis import get_spine, get_time_series

    return cs, datetime, get_spine, mo, pl, plt, timezone


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Outline of the cumulative notebook

    1. We load the full dataset
    2. We create a new table whose rows are weeks since we started collecting data.
    3. Each column is the cumulative distance ridden on a specific bike.
    4. We figure out if we need to smooth the data in any way to make the plot look better.
    """)
    return


@app.cell
def _(get_spine):
    df = get_spine(root_path="./", poll_strava=True)
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(datetime, df, pl, timezone):
    start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    f_date = pl.col("Activity Date") > start_date

    df_gear = df.filter(
        (pl.col("Activity Type") == 'Ride') & (pl.col("Activity Gear").is_not_null()) & f_date
    ).select(
        [
            pl.col("Activity ID"),
            pl.col("Activity Date"),
            pl.col("Activity Gear"),
            pl.col("Distance"),
            (pl.col("Moving Time") / 3600).alias("Hours"),
            pl.lit(1).alias("Count"),
        ]
    )
    return df_gear, start_date


@app.cell
def _(df_gear):
    bikes = df_gear["Activity Gear"].unique().to_list()
    return (bikes,)


@app.cell
def _():
    styles = ["-", "--", ":", "-."]
    return (styles,)


@app.cell
def _(cs, df_gear, pl):
    cumulative = (
        df_gear
        .with_columns(pl.col("Activity Date").dt.truncate("1w").alias("Week"))
        .group_by("Week", "Activity Gear")
        .agg(pl.col("Distance").sum())
        .pivot(on="Activity Gear", index="Week", values="Distance")
        .sort("Week")
        .upsample(time_column="Week", every="1w")
        .fill_null(0)
        .with_columns(cs.numeric().cum_sum())
    )

    cumulative_time = (
        df_gear
        .with_columns(pl.col("Activity Date").dt.truncate("1w").alias("Week"))
        .group_by("Week", "Activity Gear")
        .agg(pl.col("Hours").sum())
        .pivot(on="Activity Gear", index="Week", values="Hours")
        .sort("Week")
        .upsample(time_column="Week", every="1w")
        .fill_null(0)
        .with_columns(cs.numeric().cum_sum())
    )

    cumulative_count = (
        df_gear
        .with_columns(pl.col("Activity Date").dt.truncate("1w").alias("Week"))
        .group_by("Week", "Activity Gear")
        .agg(pl.col("Count").sum())
        .pivot(on="Activity Gear", index="Week", values="Count")
        .sort("Week")
        .upsample(time_column="Week", every="1w")
        .fill_null(0)
        .with_columns(cs.numeric().cum_sum())
    )
    return cumulative, cumulative_count, cumulative_time


@app.cell
def _(bikes, cumulative, plt, start_date, styles):
    def plot_cumulative_distance():
        plt.figure(figsize=(20, 10))

        for i, bike in enumerate(bikes):
            plt.plot(
                cumulative["Week"],
                cumulative[bike],
                linestyle=styles[i % len(styles)],
                # markersize=10,
                linewidth=2,
                label=bike,
            )

        plt.xlabel('Week')
        plt.ylabel('Cumulative distance in KM')

        plt.legend()
        plt.grid()

        plt.title(f"Cumulative distance on each bike since {start_date.date()}")

        return plt.gca()

    plot_cumulative_distance()
    return


@app.cell
def _(bikes, cumulative_time, plt, start_date, styles):
    def plot_cumulative_time():
        plt.figure(figsize=(20, 10))

        for i, bike in enumerate(bikes):
            plt.plot(
                cumulative_time["Week"],
                cumulative_time[bike],
                linestyle=styles[i % len(styles)],
                # markersize=10,
                linewidth=2,
                label=bike,
            )

        plt.xlabel('Week')
        plt.ylabel('Cumulative time in hours')

        plt.legend()
        plt.grid()

        plt.title(f"Cumulative time on each bike since {start_date.date()}")

        return plt.gca()

    plot_cumulative_time()
    return


@app.cell
def _(bikes, cumulative_count, plt, start_date, styles):
    def plot_cumulative_count():
        plt.figure(figsize=(20, 10))

        for i, bike in enumerate(bikes):
            plt.plot(
                cumulative_count["Week"],
                cumulative_count[bike],
                linestyle=styles[i % len(styles)],
                # markersize=10,
                linewidth=2,
                label=bike,
            )

        plt.xlabel('Week')
        plt.ylabel('Cumulative bike ride counts')

        plt.legend()
        plt.grid()

        plt.title(f"Cumulative number of rides on each bike since {start_date.date()}")

        return plt.gca()

    plot_cumulative_count()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
