"""
This module fits the hyperparameter tau and alpha used in the PacingModel.

It load up the entire dataset, and fits the PacingModel to the dataset for each
choice of (tau, alpha), and pick the one that minimizes the loss function we specify
in the module.
"""

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

import polars as pl
import numpy as np

from strava_history_analysis import get_spine
from strava_history_analysis.time_series_functions import (
    compute_peak_normalized_power,
    compute_peak_average_power,
    compute_normalized_power,
)
from strava_history_analysis.pacing_calculator import PacingModel

# Range of hyperparameters we search over
TAU_RANGE = np.linspace(0.4, 0.6, 40)
ALPHA_RANGE = np.linspace(0.04, 0.07, 40)
STICKINESS_RANGE = np.linspace(0.5, 200, 100)

# Some initialization values for the params we fit
anaerobic_power = 373
watts_scaling_factor = 224
cov = np.diag([100**2, 30**2])


_WORKER_DFNPF: pl.DataFrame | None = None


def _init_worker(dfnpf: pl.DataFrame):
    global _WORKER_DFNPF
    _WORKER_DFNPF = dfnpf


def _evaluate(args):
    i, j, k, tau, alpha, stickiness = args
    loss = get_hyperparameter_loss(_WORKER_DFNPF, tau, alpha, stickiness)
    return i, j, k, loss


def _parallel_grid_search(
    dfnpf: pl.DataFrame,
    tau_axis: np.ndarray,
    alpha_axis: np.ndarray,
    stickiness_axis: np.ndarray,
) -> np.ndarray:
    tasks = [
        (i, j, k, float(tau), float(alpha), float(stickiness))
        for i, tau in enumerate(tau_axis)
        for j, alpha in enumerate(alpha_axis)
        for k, stickiness in enumerate(stickiness_axis)
    ]
    grid = np.full((len(tau_axis), len(alpha_axis), len(stickiness_axis)), np.nan)
    with ProcessPoolExecutor(
        initializer=_init_worker,
        initargs=(dfnpf,),
        mp_context=mp.get_context("spawn"),
    ) as pool:
        for i, j, k, loss in pool.map(_evaluate, tasks, chunksize=8):
            grid[i, j, k] = loss
    return grid


def find_optimal_hyperparams(
    root_path="./",
    poll_strava=False,
):
    """
    Loads the full dataset once, then runs a coarse-to-fine grid search over
    (tau, alpha, stickiness) in parallel. The coarse pass spans the full
    TAU_RANGE/ALPHA_RANGE/STICKINESS_RANGE; the fine pass refines within
    +/- 2 coarse-grid-steps of the coarse minimum on each axis.
    Returns (best_tau, best_alpha, best_stickiness, best_loss, fine_loss_grid,
    fine_tau_axis, fine_alpha_axis, fine_stickiness_axis).
    """
    print("Constructing the dataset")
    dfnpf = construct_dataframe(root_path=root_path, poll_strava=poll_strava)

    tau_lo, tau_hi = float(TAU_RANGE.min()), float(TAU_RANGE.max())
    alpha_lo, alpha_hi = float(ALPHA_RANGE.min()), float(ALPHA_RANGE.max())
    stick_lo, stick_hi = float(STICKINESS_RANGE.min()), float(STICKINESS_RANGE.max())

    coarse_tau = np.linspace(tau_lo, tau_hi, 10)
    coarse_alpha = np.linspace(alpha_lo, alpha_hi, 10)
    coarse_stick = np.linspace(stick_lo, stick_hi, 20)
    print("Performing coarse grid search")
    coarse_grid = _parallel_grid_search(dfnpf, coarse_tau, coarse_alpha, coarse_stick)

    ci, cj, ck = np.unravel_index(np.nanargmin(coarse_grid), coarse_grid.shape)

    tau_step = coarse_tau[1] - coarse_tau[0]
    alpha_step = coarse_alpha[1] - coarse_alpha[0]
    stick_step = coarse_stick[1] - coarse_stick[0]
    fine_tau = np.linspace(
        max(coarse_tau[ci] - 2 * tau_step, tau_lo),
        min(coarse_tau[ci] + 2 * tau_step, tau_hi),
        10,
    )
    fine_alpha = np.linspace(
        max(coarse_alpha[cj] - 2 * alpha_step, alpha_lo),
        min(coarse_alpha[cj] + 2 * alpha_step, alpha_hi),
        10,
    )
    fine_stick = np.linspace(
        max(coarse_stick[ck] - 2 * stick_step, stick_lo),
        min(coarse_stick[ck] + 2 * stick_step, stick_hi),
        20,
    )
    print(
        f"Performing fine grid search for "
        f"alpha in [{fine_alpha[0]}, {fine_alpha[-1]}], "
        f"tau in [{fine_tau[0]}, {fine_tau[-1]}], "
        f"stickiness in [{fine_stick[0]}, {fine_stick[-1]}]"
    )
    fine_grid = _parallel_grid_search(dfnpf, fine_tau, fine_alpha, fine_stick)

    fi, fj, fk = np.unravel_index(np.nanargmin(fine_grid), fine_grid.shape)
    best_tau = float(fine_tau[fi])
    best_alpha = float(fine_alpha[fj])
    best_stickiness = float(fine_stick[fk])
    best_loss = float(fine_grid[fi, fj, fk])

    return (
        best_tau,
        best_alpha,
        best_stickiness,
        best_loss,
        fine_grid,
        fine_tau,
        fine_alpha,
        fine_stick,
    )


def get_hyperparameter_loss(
    dfnpf: pl.DataFrame,
    tau: np.float64,
    alpha: np.float64,
    stickiness: np.float64,
):
    baseline_model = PacingModel(
        anaerobic_work=anaerobic_power,
        watts_scaling_factor=watts_scaling_factor,
        covariance_matrix=cov,
        tau=tau,
        alpha=alpha,
        stickiness=stickiness,
    )

    predictions = []
    # two_hour_predictions = []

    # Iterating through all the activities and updating the pacing calculator
    for activity in dfnpf.iter_rows(named=True):
        ride_duration = activity["Moving Time"] / 60
        predicted_normalized_power = baseline_model.predict_peak_power(ride_duration)
        predictions.append(predicted_normalized_power)
        # predicted_two_hour_normalized_power = baseline_model.predict_peak_power(120)
        # two_hour_predictions.append(predicted_two_hour_normalized_power)

        censored_observations = []
        uncensored_observations = []

        censored_observations.append((5, activity["Peak 5m average power"]))
        censored_observations.append((10, activity["Peak 10m average power"]))
        censored_observations.append((20, activity["Peak 20m average power"]))
        censored_observations.append((60, activity["Peak 60m average power"]))
        censored_observations.append((120, activity["Peak 120m average power"]))

        uncensored_observations.append((60, activity["Peak 1h normalized power"]))
        uncensored_observations.append((120, activity["Peak 2h normalized power"]))

        censored_observations = list(
            filter(lambda f: f[1] is not None, censored_observations)
        )
        uncensored_observations = list(
            filter(lambda f: f[1] is not None, uncensored_observations)
        )

        baseline_model.update_based_on_observations(
            censored_observations, uncensored_observations
        )

    warmup = 10
    mse = (
        (dfnpf["Normalized power"][warmup:] - pl.Series(predictions[warmup:])) ** 2
    ).mean()
    # + (
    #     (dfnpf["Peak 2h normalized power"] - pl.Series(two_hour_predictions)) ** 2
    # ).mean()
    return mse


def construct_dataframe(
    root_path="./",
    poll_strava=False,
):
    df = get_spine(root_path=root_path, poll_strava=poll_strava).select(
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
                lambda f: compute_normalized_power(f, root_path),
                return_dtype=pl.Float64,
            )
            .alias("Normalized power"),
        ]
    )

    f_valid = pl.col("Peak 1h normalized power").is_finite()

    return dfnp.filter(f_valid)
