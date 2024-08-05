```python
from kamodo_dask.dask_config import client
```

```python
import pandas as pd
```

```python
client
```

```python
# from kamodo_dask.kamodo_dask import df_from_dask,
from kamodo_dask.dask_config import client, storage_options
from kamodo_dask.kamodo_dask import df_from_dask, KamodoDask, fetch_file_range, filter_partition
from kamodo_dask.kamodo_dask import check_file_existence, dd, PARQUET_ENGINE, CancelledError

import os

parquet_endpoint = os.environ.get('PARQUET_ENDPOINT')

print(parquet_endpoint)

# run this every time you want to fetch new data
start = pd.Timestamp.utcnow() - pd.Timedelta(days=2)
# start = pd.Timestamp(2024, 4, 9, 5, 19)

# fetch up to n hours of data
hours_of_data = 2
end = start + pd.Timedelta(seconds=hours_of_data*60*60)

# set range of altitude to fetch
h_start, h_end = 292500.0, 357500.0

# df = df_from_dask(parquet_endpoint, start, end, h_start, h_end)
```

```python
round_time='10T'
start = start.floor(round_time)
end = end.ceil(round_time)
start, end
```

Fetch a 4D dataframe using the parquet endpoint


## Last converted date

```python
start
```

```python
end - start
```

```python
parquet_endpoint
```

```python
filenames, date_range = fetch_file_range(start, end, parquet_endpoint, '.parquet')
```

```python
filenames
```

```python
parquet_endpoint
```

```python
def df_from_dask(client, endpoint, storage_options, start, end, h_start, h_end, round_time='10T', suffix='.parquet', npartitions=None, partition_size=None, verbose=False):
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
        ddf = ddf.map_partitions(filter_partition, h_range=h_range, meta=meta)

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
                print(e)
                print(f"Attempt {attempts}. Retrying...")
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
        if 'ddf' in locals():
            client.cancel(ddf)  # Ensure cleanup on any exception, only if ddf is defined
        print(f"Error processing data: {e}")
        raise
```

```python
filenames, date_range = fetch_file_range(start, end, parquet_endpoint, '.parquet')
```

```python
filenames
```

```python
date_range
```

## Trying new height partitioning

```python
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
```

```python
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
    
    print(dd)
    # Create Dask DataFrame from the list of Parquet files
    ddf = dd.read_parquet(filenames, engine=engine, storage_options=storage_options)
    
    if verbose:
        print(f"Number of partitions: {ddf.npartitions}")
    
    return ddf

ddf = parquet_to_ddf(filenames, storage_options=storage_options, engine='pyarrow', verbose=True)
```

```python
ddf
```

```python
ddf.head()
```

```python
h_range = h_start, h_end
```

```python
meta = ddf._meta
ddf_filtered = ddf.map_partitions(filter_partition, h_range=h_range, meta=meta)
```

```python
# Persist the filtered DataFrame
ddf_filtered = client.persist(ddf_filtered)
```

```python
h_range
```

```python
# Compute the result to get a Pandas DataFrame
result = ddf_filtered.compute()

# Print the resulting Pandas DataFrame
print(result)
```

```python
df = result
```

```python
len(df)//len(date_range)
```

```python
import numpy as np
```

```python
# Post-processing steps
repetitions = len(df) // len(date_range)
times = np.repeat(date_range, repetitions)
lat_values = df.index.get_level_values('lat')
lon_values = df.index.get_level_values('lon')
h_values = df.index.get_level_values('h')

new_tuples = list(zip(times, lon_values, lat_values, h_values))
new_index = pd.MultiIndex.from_tuples(new_tuples, names=["time", "lon", "lat", "h"])
df = df.set_index(new_index)
```

```python
df
```

```python
# df = df_from_dask(client, parquet_endpoint, storage_options,
#                   start, end, h_start, h_end, partition_size='200MB', verbose=True)
```

```python
df.head()
```

Construct a Kamodo object using the retrieved data

```python
df.index.levels
```

```python
kd = KamodoDask(df)
```

```python
midpoint = kd.get_midpoint()
midpoint
```

```python
kd.rho_ijkl(time=midpoint['time'], lat=0, lon=0)
```

```python
import plotly.graph_objs as go
from plotly.offline import init_notebook_mode
init_notebook_mode(connected=True)
```

## Plot curve

```python
midpoint = kd.get_midpoint()
```

```python
kd.get_bounds()
```

```python
kd
```

```python
midpoint['h']
```

```python
kd.rho_ijkl(lon=200, lat=0, h=midpoint['h'])
```

```python
kd.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(lon=200, lat=0,  h=midpoint['h'])))
```

## plot slice

```python
midpoint['lon']
```

```python
midpoint
```

```python
kd.df
```

```python
kd.rho_ijkl(time=midpoint['time'], lon=midpoint['lon']).shape
```

```python
## this will not render if there's only 3 values in altitude
```

```python
kd.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(time=midpoint['time'], lon=midpoint['lon'])))
```

```python
kd.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(time=midpoint['time'], h=midpoint['h'])))
```

```python
kd.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(time=midpoint['time'], lon=180)))
```

```python

```
