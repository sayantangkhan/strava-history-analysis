"""
Docstring for time_series_parser

In this module, we parse fit files and strava time series streams into a common time series format.
The common format will be a polars DataFrame.
"""

import fitparse
import json
import os
import polars as pl

# Listing out fields from the fit file I want to ignore for now
FIT_FILE_FIELDS_TO_IGNORE = {
    "left_right_balance (None)",
}


def parse_fit_file(fit_file_path: str) -> pl.DataFrame:
    """
    - We first iterate through the messages in the fit file
    - Filter to the ones of record type
    - Each record is a time snapshot of measured values
    - We iterate through the fields in that record, and populate the appropriate column in the dataframe
    """
    parsed_fit_file = fitparse.FitFile(fit_file_path)

    dataframe = {}
    first_row_fields = next(parsed_fit_file.get_messages("record")).fields
    for f in first_row_fields:
        f = f.as_dict()
        combined_name = f"{f['name']} ({f['units']})"
        dataframe[combined_name] = []

    for index, record in enumerate(parsed_fit_file.get_messages("record")):
        for f in record.fields:
            f = f.as_dict()
            combined_name = f"{f['name']} ({f['units']})"
            value = f["value"]

            # If this field is something we've not seen before, we populate it with None values until this point
            if combined_name not in dataframe:
                dataframe[combined_name] = [None] * index
            dataframe[combined_name].append(value)
        for _, v in dataframe.items():
            # Checking if some field was missing values in this record
            # If so, we populate with None, and hope polars knows how to deal with it
            if len(v) < index + 1:
                v.append(None)

    # Some of the fields are recorded at 2x the frequency, but with null values for half the recordings
    # we filter those out. It's unclear if it's the even or odd values recordings with the true values.
    true_length = len(dataframe["timestamp (None)"])
    for k, v in dataframe.items():
        if len(v) == 2 * true_length:
            v_prime = [r for (i, r) in enumerate(v) if i % 2 == 0]
            v_prime_prime = [r for (i, r) in enumerate(v) if i % 2 == 1]
            if all(r is None for r in v_prime):
                dataframe[k] = v_prime_prime
            elif all(r is None for r in v_prime_prime):
                dataframe[k] = v_prime
            else:
                raise ValueError("Both even and odd substreams had values")

    fields_to_remove = FIT_FILE_FIELDS_TO_IGNORE.intersection(dataframe.keys())
    for f in fields_to_remove:
        del dataframe[f]

    return pl.DataFrame(dataframe)


def parse_strava_series(series_file_path: str) -> pl.DataFrame:
    """
    We iterate through the fields specified in the json, with the different json keys corresponding to the different
    columns in the dataframe.
    """
    with open(series_file_path) as f:
        parsed_json_file = json.load(f)

    dataframe = {}
    for f in parsed_json_file.keys():
        dataframe[f] = parsed_json_file[f]["data"]

    return pl.DataFrame(dataframe)


def get_time_series(file_path: str, root_path: str = "./") -> pl.DataFrame:
    """
    Returns the time series data for an activity, using a parquet cache to avoid re-parsing.

    :param file_path: Relative path to the source file (e.g., "fit_files/123.fit")
    :param root_path: Root directory of the project
    :return: Parsed time series as a DataFrame
    """
    full_source_path = os.path.join(root_path, file_path)

    # Derive cache path: fit_files/foo.fit -> cache/foo.parquet
    cache_relative = (
        file_path.replace("fit_files/", "cache/")
        .replace(".fit", ".parquet")
        .replace(".json", ".parquet")
    )
    cache_path = os.path.join(root_path, cache_relative)

    if os.path.exists(cache_path):
        return pl.read_parquet(cache_path)

    # Parse based on file type
    if file_path.endswith(".fit"):
        df = parse_fit_file(full_source_path)
    else:
        df = parse_strava_series(full_source_path)

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    df.write_parquet(cache_path)
    return df
