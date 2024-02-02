from dask.distributed import Client
import dask
import os
import pandas as pd
import dask.dataframe as dd
import warnings

# Ignore FutureWarning from fastparquet
warnings.filterwarnings('ignore', category=FutureWarning)

# Dask scheduler (automatically manages workers)
scheduler_host = os.environ.get('SCHEDULER_HOST')
print(f'kamodo-dask connecting to scheduler_host: {scheduler_host}')
client = Client(scheduler_host)


storage_options = {
    'key': os.environ.get('ACCESS_KEY'),
    'secret': os.environ.get('SECRET_KEY')
}

def fetch_file_range(start, end, prefix, postfix, freq='10T'):
    # Generate filenames matching list of dates
    date_range = pd.date_range(start, end, freq=freq)
    file_format = f'{prefix}%Y-%m-%dT%H:%M:%S{postfix}' #2024-01-10T17:50:00
    return date_range.strftime(file_format).to_list()


# Function to get the Parquet file buffer
def get_parquet_buffer(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='fastparquet', index=False)
    buffer.seek(0)
    return buffer

def filter_partition(df, h_range):
    # Convert the 'h' level of the index to a separate column for filtering
    df['h_temp'] = pd.to_numeric(df.index.get_level_values('h'), errors='coerce')
    
    # Filter by altitude. get the closest available data that bounds the query
    h_min, h_max = h_range
    h_lower = df['h_temp'][(df['h_temp'] < h_min)].max()
    h_upper = df['h_temp'][(df['h_temp'] > h_max)].min()
    filtered_df = df[df['h_temp'].between(h_lower, h_upper)] # inclusive

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
    print(f'start: {start}, end: {end}')

    filenames = fetch_file_range(start, end, endpoint, suffix)
    if len(filenames) > 0:
        print(f'filenames: {filenames[0]} -> {filenames[-1]}')

    # Read each file into a Dask DataFrame and add a 'filename' column
    ddfs = []
    for filename in filenames:
        df = dd.read_parquet(filename, engine='fastparquet', storage_options=storage_options)
        df['filename'] = filename
        ddfs.append(df)

    # Concatenate all Dask DataFrames
    ddf = dd.concat(ddfs)

    # Function to apply timestamp to a partition
    def apply_timestamp(partition):
        # Extract filename from the partition
        unique_filenames = partition['filename'].unique()
        if len(unique_filenames) != 1:
            raise ValueError("Multiple filenames in a single partition")
        filename = unique_filenames[0]  # Use standard indexing for NumPy array

        # Extract and apply timestamp
        timestamp = extract_timestamp_from_filename(filename, f'{density_files_3d}{prefix}', '.parquet')
        return add_timestamp_to_partition(partition, timestamp)

    # Define metadata for the output DataFrame
    meta = ddf._meta.assign(timestamp=pd.Timestamp('now'))

    # Apply timestamps to each partition and provide meta
    ddf = ddf.map_partitions(apply_timestamp, meta=meta)

    # Filter the DataFrame
    filtered_ddf = ddf.map_partitions(filter_partition, h_range=h_range, meta=meta)

    # Compute the result to get a Pandas DataFrame
    df = filtered_ddf.compute()

    # Reset the existing multi-index
    df.reset_index(inplace=True)

    # Set the new multi-index with 'timestamp' as the first level
    df.set_index(['timestamp', 'lon', 'lat', 'h'], inplace=True)

    # Drop the 'filename' column
    df.drop(columns=['filename'], inplace=True)

