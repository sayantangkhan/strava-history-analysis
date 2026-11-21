import marimo

__generated_with = "0.25.2"
app = marimo.App(width="full", app_title="Activity analytics")


@app.cell
def _():
    import marimo as mo

    import polars as pl
    import matplotlib.pyplot as plt

    from strava_history_analysis import get_spine
    from strava_history_analysis.activity_time_series import (
        compute_rolling_np_for_activity,
        rolling_mean,
    )

    return compute_rolling_np_for_activity, get_spine, mo, pl, plt, rolling_mean


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Outline of the activity analytics notebook

    1. We load the spine, pulling any new activities from Strava.
    2. We filter it down to the activities we want to look at (by default, the last ride).
    3. For each one we load its uniform time series and compute rolling NP, smoothed HR and speed.
    4. We plot rolling NP, HR, speed and elevation against time, one colour per activity.
    """)
    return


@app.cell
def _(get_spine):
    df = get_spine(root_path="./", poll_strava=True)
    return (df,)


@app.cell
def _(df):
    df.tail(10)
    return


@app.cell
def _(df, pl):
    # The activities to plot. Default: the most recent ride. Some alternatives:
    #   the last 3 rides:     df.filter(pl.col("Activity Type") == "Ride").tail(3)
    #   specific activities:  df.filter(pl.col("Activity ID").is_in([20900737004, 21050481735]))
    #   by name:              df.filter(pl.col("Activity Name").str.contains("200k"))
    activities = df.filter(pl.col("Activity Type") == "Ride").tail(1)

    activities.select(
        "Activity ID", "Activity Date", "Activity Name", "Distance", "Activity Gear"
    )
    return (activities,)


@app.cell
def _():
    np_seconds = 1800  # rolling NP window
    smooth_seconds = 300  # smoothing for HR and speed
    moving_only = True  # True: drop stopped samples, x-axis is moving time
    return moving_only, np_seconds, smooth_seconds


@app.cell
def _(
    activities,
    compute_rolling_np_for_activity,
    df,
    moving_only,
    np_seconds,
    pl,
    rolling_mean,
    smooth_seconds,
):
    x_column = "moving_time" if moving_only else "elapsed"


    def load_series(activity_id):
        series = compute_rolling_np_for_activity(
            activity_id, np_seconds, df, moving_only=moving_only
        )
        return series.with_columns(
            (pl.col(x_column).dt.total_seconds() / 3600).alias("hours"),
            rolling_mean("heartrate", smooth_seconds).alias("hr_smooth"),
            (rolling_mean("speed", smooth_seconds) * 3.6).alias("kph_smooth"),
        )


    all_series = {
        activity_id: load_series(activity_id)
        for activity_id in activities["Activity ID"]
    }
    return (all_series,)


@app.cell
def _(activities, all_series, moving_only, np_seconds, plt):
    # One colour per activity, in this order. Past 8 activities, the rest are grey.
    colors = [
        "#2a78d6",
        "#eb6834",
        "#1baf7a",
        "#eda100",
        "#e87ba4",
        "#008300",
        "#6250d6",
        "#e34948",
    ]

    # (column, y-axis label), one panel each, top to bottom
    panels = [
        ("rolling_np", f"Rolling {np_seconds // 60}-min NP (W)"),
        ("hr_smooth", "Heart rate (bpm)"),
        ("kph_smooth", "Speed (km/h)"),
        ("altitude", "Elevation (m)"),
    ]


    def plot_activities():
        fig, axes = plt.subplots(len(panels), 1, figsize=(20, 16), sharex=True)

        for i, row in enumerate(activities.iter_rows(named=True)):
            series = all_series[row["Activity ID"]]
            color = colors[i] if i < len(colors) else "grey"
            label = f"{row['Activity Name']} ({row['Activity Date']:%Y-%m-%d})"

            for ax, (column, _ylabel) in zip(axes, panels):
                ax.plot(
                    series["hours"], series[column], color=color, linewidth=1.5, label=label
                )

        for ax, (_column, ylabel) in zip(axes, panels):
            ax.set_ylabel(ylabel)
            ax.grid(alpha=0.3)

        axes[-1].set_xlabel("Moving time (h)" if moving_only else "Elapsed time (h)")

        if activities.height > 1:
            axes[0].legend()
        else:
            fig.suptitle(label)

        fig.tight_layout()
        return fig


    plot_activities()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
