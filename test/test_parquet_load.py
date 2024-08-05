"""
loads 2 hrs of parquet files from memory using fixed altitude range

run from kamodo/daskdev

This version defines the filter_partition function on the client
(seems to work better when the worker/schduler are different images from the client)
"""

import os
import pandas as pd
from dask.distributed import Client
import dask.dataframe as dd
from kamodo_dask.dask_config import client, storage_options
from kamodo_dask.kamodo_dask import df_from_parquet, fetch_file_range
import numpy as np

# Define the filter_partition function within the script
def filter_partition(df, h_range):
    # Convert the 'h' level of the index to a separate column for filtering
    df['h_temp'] = pd.to_numeric(df.index.get_level_values('h'), errors='coerce')
    
    # Determine the bounds for filtering
    h_min, h_max = h_range
    
    # Ensure filtering within the specified bounds
    filtered_df = df[df['h_temp'].between(h_min, h_max, inclusive='both')]
    
    # Drop the temporary column
    filtered_df = filtered_df.drop(columns=['h_temp'])

    return filtered_df

def parquet_to_ddf(filenames, storage_options=None, engine='pyarrow', verbose=False):
    """
    Constructs a Dask DataFrame from a list of Parquet files.

    Parameters:
    - filenames: List of Parquet file paths.
    - storage_options: Dictionary of storage options to pass to the backend file system (e.g., S3 options).
    - engine: Parquet engine to use ('pyarrow' or 'fastparquet').
    - verbose: If True, print additional information.

    Returns:
    - ddf: Dask DataFrame.
    """
    if verbose:
        print("Initializing Dask DataFrame with filenames:")
        for filename in filenames:
            print(filename)

    # Create Dask DataFrame from the list of Parquet files
    ddf = dd.read_parquet(filenames, engine=engine, storage_options=storage_options)

    if verbose:
        print(f"Number of partitions: {ddf.npartitions}")

    return ddf

def df_from_parquet(client, parquet_endpoint, storage_options, engine, start, end, h_start, h_end):
    filenames, date_range = fetch_file_range(start, end, parquet_endpoint, '.parquet')

    ddf = parquet_to_ddf(filenames, storage_options=storage_options, engine=engine)

    h_range = h_start, h_end

    meta = ddf._meta
    ddf_filtered = ddf.map_partitions(filter_partition, h_range=h_range, meta=meta)

    # Persist the filtered DataFrame
    ddf_filtered = client.persist(ddf_filtered)

    # Compute the result to get a Pandas DataFrame
    df = ddf_filtered.compute()

    # Post-processing steps
    repetitions = len(df) // len(date_range)
    times = np.repeat(date_range, repetitions)
    lat_values = df.index.get_level_values('lat')
    lon_values = df.index.get_level_values('lon')
    h_values = df.index.get_level_values('h')

    new_tuples = list(zip(times, lon_values, lat_values, h_values))
    new_index = pd.MultiIndex.from_tuples(new_tuples, names=["time", "lon", "lat", "h"])
    df = df.set_index(new_index)

    return df

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
    df = df_from_parquet(client, parquet_endpoint, storage_options, 'pyarrow', start, end, h_start, h_end)

    # Print the resulting dataframe
    print(df)

if __name__ == "__main__":
    main()

