import dask
import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
import re

from kamodo import Kamodo, kamodofy, gridify
from scipy.interpolate import RegularGridInterpolator
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dask.distributed import CancelledError

from .dask_config import s3_client


PARQUET_ENGINE = os.environ.get('PARQUET_ENGINE', 'fastparquet')

# Ignore FutureWarning from fastparquet
warnings.filterwarnings('ignore', category=FutureWarning)


def check_existence(bucket, key):
    """Function to check existence of a single file."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False

def check_file_existence(filenames, prefix, postfix):
    """Check if files exist in the bucket using concurrent requests and return their timestamps, preserving order."""
    # Initialize a list to hold the results with the same length as filenames
    results = [None] * len(filenames)

    tasks = []
    for index, filename in enumerate(filenames):
        fname_array = filename.split('//')[1].split('/')
        parquet_bucket = fname_array[0]
        parquet_fname = '/'.join(fname_array[1:])
        tasks.append((index, parquet_bucket, parquet_fname, filename))

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_details = {executor.submit(check_existence, task[1], task[2]): task for task in tasks}
        for future in as_completed(future_to_details):
            index, _, _, full_filename = future_to_details[future]
            if future.result():
                datetime_str = full_filename.replace(prefix, '').replace(postfix, '')
                try:
                    timestamp = pd.to_datetime(datetime_str, format='%Y-%m-%dT%H:%M:%S')
                    # Store the result at the corresponding index to preserve order
                    results[index] = (full_filename, timestamp)
                except ValueError as e:
                    print(f"Error parsing {datetime_str}: {e}")
                    # If there's an error parsing, you could decide to set a specific value here

    # Filter out None values in case some files didn't exist or couldn't be parsed
    existing_files_with_times = [result for result in results if result is not None]

    return existing_files_with_times


def fetch_file_range(start, end, prefix, postfix, freq='10T'):
    # Generate filenames matching list of dates
    date_range = pd.date_range(start, end, freq=freq)
    file_format = f'{prefix}%Y-%m-%dT%H:%M:%S{postfix}'
    potential_filenames = date_range.strftime(file_format).to_list()

    # Check which filenames actually exist and get their timestamps
    existing_filenames_with_times = check_file_existence(potential_filenames, prefix, postfix)

    if len(existing_filenames_with_times) != len(potential_filenames):
        print(f'existing filenames {len(existing_filenames_with_times)} do not match requested {len(potential_filenames)}')

    if not existing_filenames_with_times:
        return [], None

    # Separate filenames and timestamps
    existing_filenames, timestamps = zip(*existing_filenames_with_times)
    
    if timestamps:
        reduced_date_range = pd.date_range(start=min(timestamps), end=max(timestamps), freq=freq)
    else:
        reduced_date_range = None

    return list(existing_filenames), reduced_date_range


# Function to get the Parquet file buffer
def get_parquet_buffer(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine=PARQUET_ENGINE, index=False)
    buffer.seek(0)
    return buffer


def extract_timestamp_from_filename(filename, prefix, postfix):
    # Extract the timestamp from the filename
    # Adjust this function based on your filename format
    timestamp_str = filename.replace(prefix, '').replace(postfix, '')
    return pd.to_datetime(timestamp_str)

def add_timestamp_to_partition(df, timestamp):
    df['timestamp'] = pd.to_datetime(timestamp)
    return df


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
        for _ in filenames:
            print(_)

    # Create Dask DataFrame from the list of Parquet files
    ddf = dd.read_parquet(filenames, engine=engine, storage_options=storage_options)

    if verbose:
        print(f"Number of partitions: {ddf.npartitions}")

    return ddf

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

def df_from_parquet(client, parquet_endpoint, storage_options, engine, start, end, h_start, h_end, filter_function=None):
    filenames, date_range = fetch_file_range(start, end, parquet_endpoint, '.parquet')

    ddf = parquet_to_ddf(filenames, storage_options=storage_options, engine=engine)

    h_range = h_start, h_end

    meta = ddf._meta

    if filter_function is not None:
        ddf_filtered = ddf.map_partitions(filter_function, h_range=h_range, meta=meta)

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

def df_from_dask(client, endpoint, storage_options, start, end, h_start, h_end, round_time='10T', suffix='.parquet', npartitions=None, partition_size=None, verbose=False):
    """this code is defunct, please use kamodo_dask.df_from_parquet"""

    if len(client.ncores()) == 0:
        raise RuntimeError("No available workers in the Dask cluster. Please ensure that your Dask cluster is properly configured and has active workers.")

    start = start.floor(round_time)
    end = end.ceil(round_time)
    h_range = h_start, h_end

    filenames, date_range = fetch_file_range(start, end, endpoint, suffix)

    if not filenames:
        raise IOError(f"No files found matching query\n start: {start}\n end: {end}")

    if verbose:
        print('initializing with filenames')
        print(filenames)

    try:
        ddf = dd.read_parquet(filenames, engine=PARQUET_ENGINE, storage_options=storage_options)

        if verbose:
            print(f"Initial number of partitions: {ddf.npartitions}")

        # Repartition if necessary to match the number of workers
        if npartitions is not None:
            if verbose:
                print(f'repartitioning from {ddf.npartitions} to {npartitions}')
            ddf = ddf.repartition(npartitions=npartitions)
            if verbose:
                print(f"Number of partitions after repartitioning: {ddf.npartitions}")
        elif partition_size is not None:
            if verbose:
                print(f'repartitioning from {ddf.npartitions} to {partition_size} MB per partition')
            ddf = ddf.repartition(partition_size=partition_size)
            if verbose:
                print(f"Number of partitions after repartitioning: {ddf.npartitions}")

        meta = ddf._meta
        ddf = ddf.map_partitions(filter_altitude, h_range=h_range, meta=meta)

        if verbose:
            print(f"Number of partitions after map_partitions: {ddf.npartitions}")

        # Ensure no implicit repartitioning is happening
        if verbose:
            print("Persisting DataFrame")
        ddf = client.persist(ddf, retries=3)

        if verbose:
            print(f"Number of partitions after persist: {ddf.npartitions}")

        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                df = ddf.compute(timeout=1200)  # Adjust timeout as needed
                break  # If compute is successful, break out of the loop
            except (TimeoutError, CancelledError) as e:
                attempts += 1
                print(f"Attempt {attempts} failed with {e}. Retrying...")
                if attempts < max_attempts:
                    # Clean up memory and rebalance data
                    client.cancel(ddf)
                    client.rebalance()
                    ddf = client.persist(ddf, retries=3)  # Persist the DataFrame again
                else:
                    if ddf is not None:
                        client.cancel(ddf)  # Cancel all associated tasks and free up resources only if ddf is defined
                    raise Exception("Max retries reached. Failing now.")

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
    except FileNotFoundError as m:
        print('files not found')
        print(filenames)
        raise
    except Exception as e:
        if ddf is not None:
            client.cancel(ddf)  # Ensure cleanup on any exception, only if ddf is defined
        print(f"Error processing data: {e}")
        raise


class KamodoDask(Kamodo):
    def __init__(self, df, fill_value=0, **kwargs):
        self.df = df
        
        self.fill_value = fill_value

        # assumes time is outermost level
        # convert to seconds since Unix Epoch since January 1st, 1970 at UTC
        self.time = np.array([v.value/1e9 for v in self.df.index.levels[0]])
        
        
        # datetime as int would be in nanoseconds. convert to seconds from epoch
        self.levels = {'time': self.time}
        self.interpolators = {}
        
        for level in df.index.levels[1:]:
            self.levels[level.name] = level.values

        super(KamodoDask, self).__init__(**kwargs)

        self.initialize_interpolators()


    def initialize_interpolators(self):
        var_shape = tuple([len(v) for v in self.levels.values()])
        var_levels = tuple(self.levels.values())

        for var_str in self.df.columns:
            # Regular expression to extract variable name and units
            match = re.search(r'(\w+)\[(.*?)\]', var_str)
            variable_name = match.group(1)
            units = match.group(2)
    
            var_data = self.df[var_str].fillna(self.fill_value).values.reshape(var_shape)
            rgi = RegularGridInterpolator(var_levels,
                                          var_data,
                                          bounds_error=False,
                                          fill_value=self.fill_value)
            @kamodofy(units=units)
            def interpolator(xvec):
                return rgi(xvec)
            
            gridify_args = self.levels

            @kamodofy(units=units, data={})
            @gridify(squeeze=True, order='C', **self.levels)
            def interpolator_ijkl(xvec):
                return rgi(xvec)

            self[variable_name] = interpolator
            self[variable_name + '_ijkl'] = interpolator_ijkl

    def get_bounds(self):
        return {k: (v.min(), v.max()) for k,v in self.levels.items()}

    def get_midpoint(self):
        return {k: v.mean() for k,v in self.levels.items()}
