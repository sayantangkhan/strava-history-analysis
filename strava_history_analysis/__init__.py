"""
Strava History Analysis

A package for analyzing Strava activity history using FIT files and the Strava API.
"""

from .database import (
    get_spine,
    initialize_db_from_strava_dump,
    update_spine_with_api_pull,
)
from .stravalib_wrapper import initialize_client
from .time_series_functions import (
    compute_peak_normalized_power,
    general_hr_adapter,
    general_power_adapter,
    normalized_power,
    peak_normalized_power,
    peak_rolling_hr,
)
from .time_series_parser import (
    get_activity_time_series,
    get_time_series,
    parse_fit_file,
    parse_strava_series,
)

__all__ = [
    "compute_peak_normalized_power",
    "general_hr_adapter",
    "general_power_adapter",
    "get_activity_time_series",
    "get_spine",
    "get_time_series",
    "initialize_client",
    "initialize_db_from_strava_dump",
    "normalized_power",
    "parse_fit_file",
    "parse_strava_series",
    "peak_normalized_power",
    "peak_rolling_hr",
    "update_spine_with_api_pull",
]
