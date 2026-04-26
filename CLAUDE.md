# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Tooling

- Dependencies are managed by `uv` (see `uv.lock`, `pyproject.toml`). Use `uv sync` to install and `uv run <cmd>` to execute anything that needs the project venv.
- Lint/format with `uv run ruff check` and `uv run ruff format`. There is no test suite.
- Notebooks under `notebooks/` are **marimo apps stored as plain `.py` files**, not Jupyter `.ipynb`. Open them with `uv run marimo edit notebooks/<file>.py`.
- The CLI entry point declared in `pyproject.toml` (`strava-history-analysis`) currently just prints a hello message — real work happens by importing the package from notebooks.

## Architecture

The project ingests Strava activity data from two sources and unifies them into a single analyzable dataset.

### The spine

`database/spine.parquet` is the canonical per-activity index (one row per activity, scalar metadata, plus a `Filename` pointer to the time-series file). It is built and grown by `strava_history_analysis.database`:

1. `initialize_db_from_strava_dump` — first-time build from the Strava bulk export CSV at `fit_files/activities.csv`. Filters to `.fit` activities only.
2. `update_spine_with_api_pull` — incremental: pulls activities newer than the last seen `Activity ID` via `stravalib`, writes per-activity time-series JSON to `fit_files/api_series_pulls/<id>.json`, and appends rows.
3. `get_spine(root_path, poll_strava)` — the main entry point: reads cache, optionally polls API for deltas, writes back. **This is the function notebooks call.**

So the spine is append-only and incrementally grown: bulk dump for history, API pulls for anything new. The `Activity ID` ordering invariant matters — `update_spine_with_api_pull` relies on `df["Activity ID"].last()` to know where to stop.

### Two time-series formats, unified via adapters

Each activity's time-series lives in one of two formats:

- **Historical (`.fit` files)** under `fit_files/activities/`, parsed by `parse_fit_file`. Column names look like `"power (watts)"`, `"heart_rate (bpm)"`, `"timestamp (None)"`.
- **API pulls (`.json` streams)** under `fit_files/api_series_pulls/`, parsed by `parse_strava_series`. Column names look like `"watts"`, `"heartrate"`, `"time"`, `"moving"`.

`get_time_series(file_path, root_path)` in `time_series_parser.py` dispatches by extension and **caches parsed output as parquet under `cache/`** (e.g. `fit_files/foo.fit` → `cache/activities/foo_fit.parquet`). Always go through `get_time_series` rather than parsing directly.

Downstream analysis must not branch on source format. Instead, `time_series_functions.general_power_adapter` / `general_hr_adapter` detect which format you have (presence of the `"moving"` column ⇒ Strava API) and produce a normalized DataFrame with stable columns: `duration`, `power`/`heartrate`, `fIsMoving`. Add new shared field types to `FIELD_NAME_MAPPINGS` in `time_series_functions.py` (the tuple is `(fit_name, strava_name)`) and route them through `fit_adapter` / `strava_api_adapter`.

### Analytics layer

`time_series_functions.py` exposes Polars expressions (`normalized_power`, `peak_normalized_power(seconds)`, `peak_average_power(seconds)`, `peak_rolling_hr(seconds)`) that take an *adapted* DataFrame and produce a scalar. The `compute_*` helpers wrap parse + adapt + reduce, and return `None` on `ColumnNotFoundError` so the pipeline tolerates activities missing power/HR. These are designed to be applied per-row over the spine (`map_elements`) to add new scalar columns.

### Pacing calculator

`pacing_calculator.py` fits a power-duration curve `P(t) = A/(t+τ) + B·t^(-α)` to the rider's history, with `A` (anaerobic capacity) and `B` (aerobic scaling) treated as Bayesian parameters with a multivariate-normal prior; `τ` and `α` are fixed hyperparameters. `PacingModel.update_based_on_observations` does a MAP update via `scipy.optimize.minimize` (BFGS), supporting both uncensored and censored (lower-bound) observations — censoring distinguishes "true peak" measurements from "the rider didn't try harder" measurements, optionally weighted by HR z-score. `hyperparameter_fit.py` is the (currently empty) home for fitting the fixed hyperparameters.

The README contains the math and motivation; `debugging_log.md` is a running list of known issues to consult/update when working in this area (current entry: log-likelihood term in the minimizer is always zero, causing slow prior updates).

## Data layout (gitignored)

These directories are **not** in git but are required at runtime — never commit them:

- `secrets/authentication.json` (Strava `client_id`/`client_secret`) and `secrets/token.json` (access/refresh tokens; `stravalib_wrapper.initialize_client` auto-refreshes when expired and writes back).
- `fit_files/` — the unzipped Strava bulk export. Contains `activities.csv`, `activities/*.fit`/`.gpx`, plus the live API time-series cache at `fit_files/api_series_pulls/`.
- `database/spine.parquet` — the spine cache.
- `cache/` — parsed parquet cache for time-series files.

All paths are relative to a configurable `root_path` (default `"./"`). Notebooks running from the repo root pass `root_path="./"`; keep this convention when writing new entry points.

## Conventions

- DataFrames are **Polars**, not pandas. Use `pl.col(...)`, lazy expressions, and `select`/`with_columns`.
