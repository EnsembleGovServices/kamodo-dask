from dask.distributed import Client, default_client, LocalCluster
import dask
import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
import re

from kamodo import Kamodo, kamodofy, gridify
from scipy.interpolate import RegularGridInterpolator
import warnings

# Ignore FutureWarning from fastparquet
warnings.filterwarnings('ignore', category=FutureWarning)

# Dask scheduler (automatically manages workers)
scheduler_host = os.environ.get('SCHEDULER_HOST')
print(f'kamodo-dask connecting to scheduler_host: {scheduler_host}')


def get_or_create_dask_client():
    try:
        # Check if running within a worker process. prevents scheduler from trying to connect to itself
        get_worker()
        is_worker = True
    except ValueError:
        is_worker = False

    # Only attempt to create a client if not running in the scheduler process
    if not is_worker:
        try:
            # Try to get the default Dask client if it already exists
            client = default_client()
        except ValueError:
            # If no client exists, try to get the scheduler address from an environment variable
            scheduler_host = os.getenv('SCHEDULER_HOST', None)
            
            if scheduler_host:
                # If the environment variable is set, use it to connect to the scheduler
                client = Client(scheduler_host)
            else:
                # If the environment variable is not set, optionally start a local cluster
                warnings.warn('SCHEDULER_HOST environment variable not set. Creating local cluster.')
                client = Client(LocalCluster())
        return client


client = get_or_create_dask_client()


storage_options = {
    'key': os.environ.get('ACCESS_KEY'),
    'secret': os.environ.get('SECRET_KEY')
}

def fetch_file_range(start, end, prefix, postfix, freq='10T'):
    # Generate filenames matching list of dates
    date_range = pd.date_range(start, end, freq=freq)
    # Example: '2024-01-10T17:50:00'
    file_format = f'{prefix}%Y-%m-%dT%H:%M:%S{postfix}'
    return date_range.strftime(file_format).to_list(), date_range


# Function to get the Parquet file buffer
def get_parquet_buffer(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='fastparquet', index=False)
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

def df_from_dask(endpoint, start, end, h_start, h_end, round_time='10T', suffix='.parquet'):
    start = start.floor(round_time)
    end = end.ceil(round_time)

    h_range = h_start, h_end # floor/cel is handled in filter_partion
    # print(f'start: {start}, end: {end}')

    filenames, date_range = fetch_file_range(start, end, endpoint, suffix)

    # if len(filenames) > 0:
    #     print(f'filenames: {filenames[0]} -> {filenames[-1]}')

    # Read Parquet files using Dask - leveraging the ability to read multiple files at once
    ddf = dd.read_parquet(filenames, engine='fastparquet', storage_options=storage_options)

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
    new_tuples = list(zip(times, lat_values, lon_values, h_values))

    # Create the new MultiIndex
    new_index = pd.MultiIndex.from_tuples(new_tuples, names=["time", "lat", "lon", "h"])
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
