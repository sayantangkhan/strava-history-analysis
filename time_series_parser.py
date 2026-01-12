"""
Docstring for time_series_parser

In this module, we parse fit files and strava time series streams into a common time series format.
The common format will be a polars DataFrame.
"""

import fitparse
import json
import polars as pl


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
            if not combined_name in dataframe.keys():
                dataframe[combined_name] = [None] * index
            dataframe[combined_name].append(value)
        for _, v in dataframe.items():
            # Checking if some field was missing values in this record
            # If so, we populate with None, and hope polars knows how to deal with it
            if len(v) < index + 1:
                v.append(None)

    return pl.DataFrame(dataframe)
