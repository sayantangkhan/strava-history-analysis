"""
Docstring for database

This module contains the basic functions we will use to create/update the database of fit file associated activities
"""

import polars as pl
import os
from stravalib_wrapper import initialize_client
import json


def initialize_db_from_strava_dump(root_path="./"):
    strava_supplied_dataset = pl.read_csv(
        os.path.join(root_path, "fit_files", "activities.csv")
    )

    base_spine = [
        pl.col("Activity ID"),
        pl.col("Activity Date")
        .str.to_datetime("%b %-d, %Y, %-I:%M:%S %p")
        .dt.replace_time_zone("UTC"),
        pl.col("Activity Type"),
        pl.col("Activity Name"),
        pl.col("Activity Gear"),
        pl.col("Commute"),
        pl.col("Elapsed Time").cast(pl.Int64),
        pl.col("Moving Time").cast(pl.Int64),
        pl.col("Distance").cast(pl.Float64),
        pl.col("Average Speed").cast(pl.Float64),
        pl.col("Elevation Gain").cast(pl.Float64),
        pl.col("Average Heart Rate").cast(pl.Float64),
        pl.col("Max Heart Rate").cast(pl.Float64),
        pl.col("Average Cadence").cast(pl.Float64),
        (pl.lit("fit_files/") + pl.col("Filename").str.strip_suffix(".gz")).alias(
            "Filename"
        ),
    ]

    fit_filter = pl.col("Filename").str.ends_with(".fit")

    return strava_supplied_dataset.select(base_spine).filter(fit_filter)


def update_spine_with_api_pull(df: pl.DataFrame, root_path="./") -> pl.DataFrame:
    """
    Docstring for update_spine_with_api_pull

    :param df: The cached DataFrame to update with the API pull
    :type df: pl.DataFrame
    :return: Returns the updated DataFrame
    :rtype: DataFrame

    This function relies on the Activity ID being sorted in the DataFrame
    """
    last_seen_id = df["Activity ID"].last()

    client = initialize_client(root_path=root_path)
    unseen_ids = []
    datetimes = []
    timeseries_data = []
    activity_type = []
    paths = []

    activity_names = []
    activity_gear = []
    is_commute = []
    elapsed_time = []
    moving_time = []
    distance = []
    average_speed = []
    elevation_gain = []
    average_heart_rate = []
    max_heart_rate = []
    average_cadence = []

    for activity in client.get_activities():
        activity_id = activity.id
        if activity_id <= last_seen_id:
            break
        unseen_ids.append(activity_id)
        datetimes.append(activity.start_date)
        activity_type.append(activity.type.root)
        # Polling strava for the time series data associated to the activity
        activity_stream = client.get_activity_streams(activity_id)
        activity_stream = {k: v.model_dump() for k, v in activity_stream.items()}
        timeseries_data.append(activity_stream)
        # Now polling it for the usual metadata associated to the activity
        metadata = client.get_activity(activity_id).model_dump()
        activity_names.append(metadata["name"])
        activity_gear.append(metadata["gear"]["name"])
        is_commute.append(metadata["commute"])
        elapsed_time.append(metadata["elapsed_time"])
        moving_time.append(metadata["moving_time"])
        distance.append(metadata["distance"])
        average_speed.append(metadata["average_speed"])
        elevation_gain.append(metadata["total_elevation_gain"])
        average_heart_rate.append(metadata["average_heartrate"])
        max_heart_rate.append(metadata["max_heartrate"])
        average_cadence.append(metadata["average_cadence"])

    for activity_id, stream in zip(unseen_ids, timeseries_data):
        json_path_no_prefix = os.path.join(
            "fit_files", "api_series_pulls", f"{activity_id}.json"
        )

        json_path = os.path.join(
            root_path, "fit_files", "api_series_pulls", f"{activity_id}.json"
        )
        with open(json_path, "w") as f:
            json.dump(stream, f)
            paths.append(json_path_no_prefix)

    new_df = pl.DataFrame(
        {
            "Activity ID": unseen_ids,
            "Activity Date": datetimes,
            "Activity Type": activity_type,
            "Activity Name": activity_names,
            "Activity Gear": activity_gear,
            "Commute": is_commute,
            "Elapsed Time": elapsed_time,
            "Moving Time": moving_time,
            "Distance": distance,
            "Average Speed": average_speed,
            "Elevation Gain": elevation_gain,
            "Average Heart Rate": average_heart_rate,
            "Max Heart Rate": max_heart_rate,
            "Average Cadence": average_cadence,
            "Filename": paths,
        }
    ).reverse()

    new_df = new_df.with_columns(
        pl.col("Elapsed Time").cast(pl.Int64),
        pl.col("Moving Time").cast(pl.Int64),
        pl.col("Distance").cast(pl.Float64) / 1e3,
        pl.col("Average Speed").cast(pl.Float64),
        pl.col("Elevation Gain").cast(pl.Float64),
        pl.col("Average Heart Rate").cast(pl.Float64),
        pl.col("Max Heart Rate").cast(pl.Float64),
        pl.col("Average Cadence").cast(pl.Float64),
    )

    df = pl.concat([df, new_df])

    return df


def get_spine(root_path="./"):
    """
    Docstring for get_spine

    First it checks local cache for the spine db. If it doesn't exist, it creates it from the csv
    """
    cached_df_path = os.path.join(root_path, "database", "spine.parquet")
    if os.path.exists(cached_df_path):
        df = pl.read_parquet(cached_df_path)
        df = update_spine_with_api_pull(df, root_path=root_path)
        df.write_parquet(cached_df_path)
    else:
        df = initialize_db_from_strava_dump(root_path=root_path)
        df = update_spine_with_api_pull(df, root_path=root_path)
        df.write_parquet(cached_df_path)

    return df
