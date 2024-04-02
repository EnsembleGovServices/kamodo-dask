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

import boto3


PARQUET_ENGINE = os.environ.get('PARQUET_ENGINE', 'fastparquet')

# Ignore FutureWarning from fastparquet
warnings.filterwarnings('ignore', category=FutureWarning)

s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ['ACCESS_KEY'],
                         aws_secret_access_key=os.environ['SECRET_KEY'])

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

def filter_partition(df, h_range):
    # Convert the 'h' level of the index to a separate column for filtering
    df['h_temp'] = pd.to_numeric(df.index.get_level_values('h'), errors='coerce')
    
    # Determine the bounds for filtering
    h_min, h_max = h_range
    h_lower = df['h_temp'][(df['h_temp'] < h_min)].max()
    h_upper = df['h_temp'][(df['h_temp'] > h_max)].min()

    # Adjust h_lower or h_upper if they are NaN
    if pd.isna(h_lower):
        h_lower = df['h_temp'].min()
    if pd.isna(h_upper):
        h_upper = df['h_temp'].max()

    # Ensure filtering within the adjusted bounds
    filtered_df = df[df['h_temp'].between(h_lower, h_upper, inclusive='both')]

    # Drop the temporary column
    filtered_df = filtered_df.drop(columns=['h_temp'])

    return filtered_df


def extract_timestamp_from_filename(filename, prefix, postfix):
    # Extract the timestamp from the filename
    # Adjust this function based on your filename format
    timestamp_str = filename.replace(prefix, '').replace(postfix, '')
    return pd.to_datetime(timestamp_str)

def add_timestamp_to_partition(df, timestamp):
    df['timestamp'] = pd.to_datetime(timestamp)
    return df

def df_from_dask(client, endpoint, storage_options, start, end, h_start, h_end, round_time='10T', suffix='.parquet'):
    start = start.floor(round_time)
    end = end.ceil(round_time)

    h_range = h_start, h_end # floor/cel is handled in filter_partion
    # print(f'start: {start}, end: {end}')

    filenames, date_range = fetch_file_range(start, end, endpoint, suffix)

    if len(filenames) == 0:
        raise IOError(f"No files found matching query\n start: {start}\n end: {end}")

    # if len(filenames) > 0:
    #     print(f'filenames: {filenames[0]} -> {filenames[-1]}')

    # Read Parquet files using Dask - leveraging the ability to read multiple files at once
    ddf = dd.read_parquet(filenames, engine=PARQUET_ENGINE, storage_options=storage_options)

    meta = ddf._meta
    
    # Filter the DataFrame
    ddf = ddf.map_partitions(filter_partition, h_range=h_range, meta=meta)

    ddf = client.persist(ddf)
    
    # Compute the result to get a Pandas DataFrame
    df = ddf.compute()

    repetitions = len(df)//len(date_range)

    times = np.repeat(date_range, repetitions)

    lat_values = df.index.get_level_values('lat')
    lon_values = df.index.get_level_values('lon')
    h_values = df.index.get_level_values('h')


    # Create new tuples by zipping the arrays together
    new_tuples = list(zip(times, lon_values, lat_values, h_values))

    # Create the new MultiIndex
    new_index = pd.MultiIndex.from_tuples(new_tuples, names=["time", "lon", "lat", "h"])
    df = df.set_index(new_index)

    return df


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
