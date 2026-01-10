"""
Docstring for database

This module contains the basic functions we will use to create/update the database of fit file associated activities
"""

import polars as pl
import os


def initialize_db_from_strava_dump():
    strava_supplied_dataset = pl.read_csv(os.path.join("fit_files", "activities.csv"))

    base_spine = [
        pl.col("Activity ID"),
        pl.col("Activity Date").str.to_datetime("%b %-d, %Y, %-I:%M:%S %p"),
        pl.col("Filename").str.strip_suffix(".gz"),
    ]

    fit_filter = pl.col("Filename").str.ends_with(".fit")

    return strava_supplied_dataset.select(base_spine).filter(fit_filter)
