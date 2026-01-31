"""
Strava History Analysis

A package for analyzing Strava activity history using FIT files and the Strava API.
"""

from .database import get_spine, initialize_db_from_strava_dump, update_spine_with_api_pull
from .stravalib_wrapper import initialize_client
from .time_series_parser import get_time_series, parse_fit_file, parse_strava_series
from .time_series_functions import (
    compute_peak_normalized_power,
    normalized_power,
    peak_normalized_power,
    peak_rolling_hr,
    general_power_adapter,
    general_hr_adapter,
)

__all__ = [
    "get_spine",
    "initialize_db_from_strava_dump",
    "update_spine_with_api_pull",
    "initialize_client",
    "get_time_series",
    "parse_fit_file",
    "parse_strava_series",
    "compute_peak_normalized_power",
    "normalized_power",
    "peak_normalized_power",
    "peak_rolling_hr",
    "general_power_adapter",
    "general_hr_adapter",
]
