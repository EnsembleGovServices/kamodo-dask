"""
loads 2 hrs of parquet files from memory using fixed altitude range

this version attemps to load the files without defining filter_partitions in the client
"""

import os
import dask
import pandas as pd
from kamodo_dask.dask_config import client, storage_options
from kamodo_dask.kamodo_dask import df_from_parquet, fetch_file_range


def main():
    # Fetch the parquet endpoint from environment variables
    parquet_endpoint = os.environ.get('PARQUET_ENDPOINT')

    # Print the parquet endpoint to verify it was fetched correctly
    print(f"Parquet Endpoint: {parquet_endpoint}")

    # Set the start time to 2 days ago from the current UTC time
    start = pd.Timestamp.utcnow() - pd.Timedelta(days=2)

    # Fetch up to 2 hours of data
    hours_of_data = 2
    end = start + pd.Timedelta(seconds=hours_of_data * 60 * 60)

    # Set the range of altitude to fetch
    h_start, h_end = 292500.0, 357500.0

    # Round the start and end times
    round_time = '10T'
    start = start.floor(round_time)
    end = end.ceil(round_time)

    print(f"Start Time: {start}, End Time: {end}")

    # Fetch the 4D dataframe using the parquet endpoint
    df = df_from_parquet(client, parquet_endpoint, storage_options, 'pyarrow',
        start, end, h_start, h_end)

    # Print the resulting dataframe
    print(df)

if __name__ == "__main__":
    main()
