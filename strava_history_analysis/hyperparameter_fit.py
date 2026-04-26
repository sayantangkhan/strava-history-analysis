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
TAU_RANGE = np.linspace(0.1, 1.0, 100)
ALPHA_RANGE = np.linspace(0.01, 0.10, 100)


# Some initialization values for the params we fit
anaerobic_power = 373
watts_scaling_factor = 224
cov = np.diag([100**2, 30**2])


_WORKER_DFNPF: pl.DataFrame | None = None


def _init_worker(dfnpf: pl.DataFrame):
    global _WORKER_DFNPF
    _WORKER_DFNPF = dfnpf


def _evaluate(args):
    i, j, tau, alpha = args
    loss = get_hyperparameter_loss(_WORKER_DFNPF, tau, alpha)
    return i, j, loss


def _parallel_grid_search(
    dfnpf: pl.DataFrame,
    tau_axis: np.ndarray,
    alpha_axis: np.ndarray,
) -> np.ndarray:
    tasks = [
        (i, j, float(tau), float(alpha))
        for i, tau in enumerate(tau_axis)
        for j, alpha in enumerate(alpha_axis)
    ]
    grid = np.full((len(tau_axis), len(alpha_axis)), np.nan)
    with ProcessPoolExecutor(
        initializer=_init_worker,
        initargs=(dfnpf,),
        mp_context=mp.get_context("spawn"),
    ) as pool:
        for i, j, loss in pool.map(_evaluate, tasks, chunksize=8):
            grid[i, j] = loss
    return grid


def find_optimal_hyperparams(
    root_path="./",
    poll_strava=False,
):
    """
    Loads the full dataset once, then runs a coarse-to-fine grid search over
    (tau, alpha) in parallel. The coarse pass spans the full TAU_RANGE/ALPHA_RANGE;
    the fine pass refines within +/- 2 coarse-grid-steps of the coarse minimum.
    Returns (best_tau, best_alpha, best_loss, fine_loss_grid, fine_tau_axis, fine_alpha_axis).
    """
    print("Constructing the dataset")
    dfnpf = construct_dataframe(root_path=root_path, poll_strava=poll_strava)

    tau_lo, tau_hi = float(TAU_RANGE.min()), float(TAU_RANGE.max())
    alpha_lo, alpha_hi = float(ALPHA_RANGE.min()), float(ALPHA_RANGE.max())

    coarse_tau = np.linspace(tau_lo, tau_hi, 20)
    coarse_alpha = np.linspace(alpha_lo, alpha_hi, 20)
    print("Performing coarse grid search")
    coarse_grid = _parallel_grid_search(dfnpf, coarse_tau, coarse_alpha)

    ci, cj = np.unravel_index(np.nanargmin(coarse_grid), coarse_grid.shape)

    tau_step = coarse_tau[1] - coarse_tau[0]
    alpha_step = coarse_alpha[1] - coarse_alpha[0]
    fine_tau = np.linspace(
        max(coarse_tau[ci] - 2 * tau_step, tau_lo),
        min(coarse_tau[ci] + 2 * tau_step, tau_hi),
        20,
    )
    fine_alpha = np.linspace(
        max(coarse_alpha[cj] - 2 * alpha_step, alpha_lo),
        min(coarse_alpha[cj] + 2 * alpha_step, alpha_hi),
        20,
    )
    print(
        f"Performing fine grid search for alpha in [{fine_alpha[0]}, {fine_alpha[-1]}] and tau in [{fine_tau[0]}, {fine_tau[-1]}]"
    )
    fine_grid = _parallel_grid_search(dfnpf, fine_tau, fine_alpha)

    fi, fj = np.unravel_index(np.nanargmin(fine_grid), fine_grid.shape)
    best_tau = float(fine_tau[fi])
    best_alpha = float(fine_alpha[fj])
    best_loss = float(fine_grid[fi, fj])

    return best_tau, best_alpha, best_loss, fine_grid, fine_tau, fine_alpha


def get_hyperparameter_loss(
    dfnpf: pl.DataFrame,
    tau: np.float64,
    alpha: np.float64,
):
    baseline_model = PacingModel(
        anaerobic_work=anaerobic_power,
        watts_scaling_factor=watts_scaling_factor,
        covariance_matrix=cov,
        tau=tau,
        alpha=alpha,
    )

    predictions = []
    two_hour_predictions = []

    # Iterating through all the activities and updating the pacing calculator
    for activity in dfnpf.iter_rows(named=True):
        censored_observations = []
        uncensored_observations = []

        censored_observations.append((5, activity["Peak 5m average power"]))
        censored_observations.append((10, activity["Peak 10m average power"]))
        censored_observations.append((20, activity["Peak 20m average power"]))
        censored_observations.append((60, activity["Peak 60m average power"]))
        censored_observations.append((120, activity["Peak 120m average power"]))

        uncensored_observations.append((60, activity["Peak 1h normalized power"]))
        uncensored_observations.append((120, activity["Peak 2h normalized power"]))
        uncensored_observations.append(
            (activity["Moving Time"] / 60, activity["Normalized power"])
        )

        censored_observations = list(
            filter(lambda f: f[1] is not None, censored_observations)
        )
        uncensored_observations = list(
            filter(lambda f: f[1] is not None, uncensored_observations)
        )

        baseline_model.update_based_on_observations(
            censored_observations, uncensored_observations
        )

        ride_duration = activity["Moving Time"] / 60
        predicted_normalized_power = baseline_model.predict_peak_power(ride_duration)
        predicted_two_hour_normalized_power = baseline_model.predict_peak_power(120)
        predictions.append(predicted_normalized_power)
        two_hour_predictions.append(predicted_two_hour_normalized_power)

    mse = ((dfnpf["Normalized power"] - pl.Series(predictions)) ** 2).mean() + (
        (dfnpf["Peak 2h normalized power"] - pl.Series(two_hour_predictions)) ** 2
    ).mean()
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
