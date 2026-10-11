import marimo

__generated_with = "0.24.2"
app = marimo.App(width="full", app_title="Per bike leaderboards")


@app.cell
def _():
    import marimo as mo

    import polars as pl
    import numpy as np

    from strava_history_analysis import get_spine, get_time_series
    from strava_history_analysis.time_series_functions import (
        general_power_adapter,
        normalized_power,
    )

    return (
        general_power_adapter,
        get_spine,
        get_time_series,
        mo,
        normalized_power,
        np,
        pl,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Outline of the per bike leaderboards notebook

    1. We load the full dataset and restrict it to rides done on a real bike.
    2. We split off the "long ride" subset (>= `LONG_RIDE_KM` km) and compute whole
       ride normalized power for just those rides, since parsing the time series is
       the expensive part and the short rides never make the speed or power boards.
    3. For each of the three categories we take the top 10 rides per bike:
        - **Distance** over every ride.
        - **Average speed**, long rides only, so that a short fast blast doesn't
          crowd out the board.
        - **Normalized power**, long rides only, for the same reason.
    4. Each board is shown as one tab per bike.
    """)
    return


@app.cell
def _():
    LONG_RIDE_KM = 30
    TOP_N = 15
    return LONG_RIDE_KM, TOP_N


@app.cell
def _(get_spine):
    df = get_spine(root_path="./", poll_strava=True)
    return (df,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Restricting to rides on a real bike
    """)
    return


@app.cell
def _(df, pl):
    f_ride = pl.col("Activity Type").is_in(["Ride", "VirtualRide", "Virtual Ride"])
    f_real_gear = pl.col("Activity Gear").is_not_null()

    df_rides = df.filter(f_ride & f_real_gear).with_columns(
        (pl.col("Average Speed") * 3.6).alias("Speed (km/h)"),
        (pl.col("Moving Time") / 3600).alias("Hours"),
    )
    return (df_rides,)


@app.cell
def _(df_rides, pl):
    # Tab ordering: the bike you ride most goes first, and stays first on all three
    # boards rather than jumping around with whichever bike happens to hold the top ride.
    bikes = (
        df_rides["Activity Gear"]
        .value_counts()
        .sort("count", descending=True)["Activity Gear"]
        .to_list()
    )

    ride_counts = (
        df_rides.group_by("Activity Gear")
        .agg(
            pl.len().alias("Rides"),
            pl.col("Distance").sum().round(0).alias("Total km"),
        )
        .sort("Rides", descending=True)
    )

    ride_counts
    return (bikes,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Computing normalized power for the long rides

    Normalized power is taken over the moving portion of the ride only, matching the
    convention in `historical_normalized_power.py`. Rides with no power data come back
    as null and drop off the power board.
    """)
    return


@app.cell
def _(general_power_adapter, get_time_series, normalized_power, np, pl):
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

    return (compute_normalized_power,)


@app.cell
def _(LONG_RIDE_KM, compute_normalized_power, df_rides, pl):
    root_path = "./"

    df_long = df_rides.filter(pl.col("Distance") >= LONG_RIDE_KM).with_columns(
        pl.col("Filename")
        .map_elements(
            lambda f: compute_normalized_power(f, root_path),
            return_dtype=pl.Float64,
        )
        .alias("Normalized power")
    )
    return (df_long,)


@app.cell
def _(mo):
    mo.md(r"""
    ## The leaderboard machinery
    """)
    return


@app.cell
def _(TOP_N, bikes, mo, pl):
    DISPLAY_COLUMNS = (
        "Activity Gear",
        "Rank",
        "Date",
        "Activity Name",
        "Distance",
        "Moving",
        "Speed (km/h)",
        "Climbing (m)",
        "Normalized power",
    )

    def format_board(
        frame: pl.DataFrame, metric: str, n: int = TOP_N, within: str = "Activity Gear"
    ) -> pl.DataFrame:
        """Top `n` rides by `metric` within each value of `within`, ranked, with the
        display columns. Pass a constant column as `within` to rank across everything."""
        ranked = (
            frame.filter(pl.col(metric).is_not_null())
            .sort(metric, descending=True)
            .group_by(within, maintain_order=True)
            .head(n)
            .with_columns(
                pl.int_range(1, pl.len() + 1).over(within).alias("Rank"),
                pl.col("Activity Date").dt.date().alias("Date"),
                (
                    (pl.col("Moving Time") // 3600).cast(pl.String)
                    + ":"
                    + (pl.col("Moving Time") % 3600 // 60)
                    .cast(pl.String)
                    .str.pad_start(2, "0")
                ).alias("Moving"),
                pl.col("Distance").round(1),
                pl.col("Speed (km/h)").round(1),
                pl.col("Elevation Gain").round(0).alias("Climbing (m)"),
            )
        )

        # The distance board runs over every ride, including the short ones whose time
        # series we never paid to parse, so it has no power column to round or show.
        if "Normalized power" in ranked.columns:
            ranked = ranked.with_columns(pl.col("Normalized power").round(0))

        return ranked.select(c for c in DISPLAY_COLUMNS if c in ranked.columns)

    def leaderboard_tabs(frame: pl.DataFrame, metric: str, n: int = TOP_N):
        """One tab per bike, each holding that bike's top `n` rides by `metric`."""
        board = format_board(frame, metric, n)

        tabs = {}
        for bike in bikes:
            per_bike = board.filter(pl.col("Activity Gear") == bike).drop(
                "Activity Gear"
            )
            if per_bike.is_empty():
                # A bike with no qualifying rides gets no tab rather than an empty one.
                continue
            tabs[bike] = mo.ui.table(
                per_bike, selection=None, pagination=False, show_column_summaries=False
            )

        return mo.ui.tabs(tabs)

    return format_board, leaderboard_tabs


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Longest rides
    """)
    return


@app.cell
def _(df_rides, leaderboard_tabs):
    leaderboard_tabs(df_rides, "Distance")
    return


@app.cell(hide_code=True)
def _(LONG_RIDE_KM, mo):
    mo.md(f"""
    ## Fastest rides (>= {LONG_RIDE_KM} km)
    """)
    return


@app.cell
def _(df_long, leaderboard_tabs):
    leaderboard_tabs(df_long, "Speed (km/h)")
    return


@app.cell(hide_code=True)
def _(LONG_RIDE_KM, mo):
    mo.md(f"""
    ## Highest normalized power (>= {LONG_RIDE_KM} km)
    """)
    return


@app.cell
def _(df_long, leaderboard_tabs):
    leaderboard_tabs(df_long, "Normalized power")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## All time boards

    The same three categories, ignoring which bike the ride was on.
    """)
    return


@app.cell
def _(df_long, df_rides, format_board, mo, pl):
    def overall_board(frame: pl.DataFrame, metric: str) -> pl.DataFrame:
        return format_board(
            frame.with_columns(pl.lit("All").alias("All rides")),
            metric,
            within="All rides",
        ).rename({"Activity Gear": "Bike"})

    mo.ui.tabs(
        {
            "Distance": mo.ui.table(
                overall_board(df_rides, "Distance"), selection=None, pagination=False
            ),
            "Speed": mo.ui.table(
                overall_board(df_long, "Speed (km/h)"), selection=None, pagination=False
            ),
            "Normalized power": mo.ui.table(
                overall_board(df_long, "Normalized power"),
                selection=None,
                pagination=False,
            ),
        }
    )
    return


if __name__ == "__main__":
    app.run()
