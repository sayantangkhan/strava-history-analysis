import marimo

__generated_with = "0.22.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import polars as pl
    import matplotlib.pyplot as plt
    import numpy as np

    from strava_history_analysis import get_spine, get_time_series
    from strava_history_analysis.time_series_functions import (
        compute_peak_normalized_power,
        compute_peak_average_power,
        compute_average_power,
        compute_normalized_power,
    )

    return (
        compute_average_power,
        compute_normalized_power,
        compute_peak_average_power,
        compute_peak_normalized_power,
        get_spine,
        mo,
        np,
        pl,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Building the dataset to fit the pacing calculator

    1. For each ride, compute average power for entire ride, as well peak average power for 1m, 5m, 10m, 20m, 60m, 120m, as well as peak 1h and 2h NP.
    2. We will use the peak 1h and 2h NP, as well as the whole ride average power as point measurements, and the rest as censored measurements.
    """)
    return


@app.cell
def _(get_spine, pl):
    df = get_spine(root_path="./", poll_strava=False).select(
        [
            pl.col("Activity ID"),
            pl.col("Activity Date"),
            pl.col("Activity Type"),
            pl.col("Activity Name"),
            pl.col("Activity Gear"),
            pl.col("Elapsed Time"),
            pl.col("Moving Time"),
            pl.col("Filename"),
        ]
    )
    return (df,)


@app.cell
def _(
    compute_average_power,
    compute_normalized_power,
    compute_peak_average_power,
    compute_peak_normalized_power,
    df,
    pl,
):
    root_path = "./"

    dfnp = df.with_columns(
        [
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
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_average_power(60, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 1m average power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_average_power(300, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 5m average power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_average_power(600, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 10m average power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_average_power(1200, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 20m average power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_average_power(3600, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 60m average power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_peak_average_power(7200, f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Peak 120m average power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_average_power(f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Average power"),
            pl.col("Filename")
            .map_elements(
                lambda f: compute_normalized_power(f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Normalized power"),
        ]
    )
    return (dfnp,)


@app.cell
def _(pl):
    f_valid = (
        (pl.col("Peak 1h normalized power").is_finite())
    )
    return (f_valid,)


@app.cell
def _(dfnp, f_valid):
    dfnpf = dfnp.filter(f_valid)#.tail(10)
    return (dfnpf,)


@app.cell
def _():
    from strava_history_analysis.pacing_calculator import PacingModel

    return (PacingModel,)


@app.cell
def _(np):
    # Initial estimates
    anaerobic_power = 373
    tau = 0.6
    alpha = 0.054
    watts_scaling_factor = 224
    cov = np.diag([100**2, 30**2])
    stickiness = 58
    return alpha, anaerobic_power, cov, stickiness, tau, watts_scaling_factor


@app.cell
def _(
    PacingModel,
    alpha,
    anaerobic_power,
    cov,
    stickiness,
    tau,
    watts_scaling_factor,
):
    baseline_model = PacingModel(
        anaerobic_work=anaerobic_power,
        watts_scaling_factor=watts_scaling_factor,
        covariance_matrix=cov,
        tau=tau,
        alpha=alpha,
        stickiness=stickiness,
    )
    return (baseline_model,)


@app.cell
def _(baseline_model):
    baseline_model.predict_peak_power(5)
    return


@app.cell
def _():
    # activity = list(dfnpf.iter_rows(named=True))[0]
    # print(baseline_model)
    # censored_observations = []
    # uncensored_observations = []

    # censored_observations.append((5, activity['Peak 5m average power']))
    # censored_observations.append((10, activity['Peak 10m average power']))
    # censored_observations.append((20, activity['Peak 20m average power']))
    # censored_observations.append((60, activity['Peak 60m average power']))
    # censored_observations.append((120, activity['Peak 120m average power']))

    # uncensored_observations.append((60, activity['Peak 1h normalized power']))
    # uncensored_observations.append((120, activity['Peak 2h normalized power']))
    # # uncensored_observations.append((activity["Moving Time"] / 60, activity['Average power']))

    # censored_observations = list(filter(lambda f: f[1] is not None, censored_observations))
    # uncensored_observations = list(filter(lambda f: f[1] is not None, uncensored_observations))

    # print("Censored")
    # print((censored_observations))

    # print("Uncensored")
    # print((uncensored_observations))

    # baseline_model.update_based_on_observations(
    #     censored_observations,
    #     uncensored_observations
    # )

    # print(baseline_model)
    return


@app.cell
def _(baseline_model, dfnpf):
    for activity in dfnpf.iter_rows(named=True):
        censored_observations = []
        uncensored_observations = []

        censored_observations.append((5, activity['Peak 5m average power']))
        censored_observations.append((10, activity['Peak 10m average power']))
        censored_observations.append((20, activity['Peak 20m average power']))
        censored_observations.append((60, activity['Peak 60m average power']))
        censored_observations.append((120, activity['Peak 120m average power']))

        uncensored_observations.append((60, activity['Peak 1h normalized power']))
        uncensored_observations.append((120, activity['Peak 2h normalized power']))
        uncensored_observations.append((activity["Moving Time"] / 60, activity['Normalized power']))

        censored_observations = list(filter(lambda f: f[1] is not None, censored_observations))
        uncensored_observations = list(filter(lambda f: f[1] is not None, uncensored_observations))

        baseline_model.update_based_on_observations(
            censored_observations,
            uncensored_observations
        )
        p5 = baseline_model.predict_peak_power(5)
        p20 = baseline_model.predict_peak_power(20)
        p60 = baseline_model.predict_peak_power(60)

        print(f"5m = {p5}, 20m = {p20}, 60m = {p60}")
    return


@app.cell
def _(baseline_model):
    print(baseline_model)
    return


@app.cell
def _(baseline_model):
    baseline_model.predict_peak_power(60 * 6)
    return


@app.cell
def _(dfnpf):
    dfnpf
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
