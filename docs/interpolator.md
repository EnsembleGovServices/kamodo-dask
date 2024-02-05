```python
from kamodo_dask.kamodo_dask import client
```

```python
import pandas as pd
```

```python
client
```

```python
from kamodo_dask.kamodo_dask import df_from_dask

import os

parquet_endpoint = os.environ.get('PARQUET_ENDPOINT')

print(parquet_endpoint)

# run this every time you want to fetch new data
end = pd.Timestamp.utcnow() - pd.Timedelta(days=2)

# fetch up to n hours of data
hours_of_data = 1
start = end - pd.Timedelta(seconds=hours_of_data*60*60)

# set range of altitude to fetch
h_start, h_end = 292500.0, 357500.0

# df = df_from_dask(parquet_endpoint, start, end, h_start, h_end)
```

```python
from dask.distributed import Client
import dask.dataframe as dd
import os
import pandas as pd
import warnings

# Ignore FutureWarning from fastparquet
warnings.filterwarnings('ignore', category=FutureWarning)

# Connect to the Dask scheduler
scheduler_host = os.environ.get('SCHEDULER_HOST')
print(f'kamodo-dask connecting to scheduler_host: {scheduler_host}')
client = Client(scheduler_host)

storage_options = {
    'key': os.environ.get('ACCESS_KEY'),
    'secret': os.environ.get('SECRET_KEY')
}
```

```python
def fetch_file_range(start, end, prefix, postfix, freq='10T'):
    # Generate filenames matching list of dates
    date_range = pd.date_range(start, end, freq=freq)
    file_format = f'{prefix}%Y-%m-%dT%H:%M:%S{postfix}'  # Example: '2024-01-10T17:50:00'
    return date_range.strftime(file_format).to_list(), date_range

def df_from_dask(endpoint, start, end, h_start, h_end, round_time='10T', suffix='.parquet'):
    start = start.floor(round_time)
    end = end.ceil(round_time)
    
    filenames, date_range = fetch_file_range(start, end, endpoint, suffix)
    if not filenames:
        print("No filenames to process.")
        return pd.DataFrame()  # or appropriate empty response

    print(f'Processing files from {filenames[0]} to {filenames[-1]}')

    # Read Parquet files using Dask - leveraging the ability to read multiple files at once
    ddf = dd.read_parquet(filenames, engine='fastparquet', storage_options=storage_options)

    # No need to manually add 'filename' column as it might not be necessary for further processing
    # If you still need to manipulate or use the filename, consider keeping it in the DataFrame as you did

    # Assuming additional processing is required here, similar to your original code
    # For example, filtering based on 'h' value and adding timestamp columns would go here

    return ddf  # Return Dask DataFrame for further processing or computation

```

```python
import os

parquet_endpoint = os.environ.get('PARQUET_ENDPOINT')
```

```python
parquet_endpoint
```

```python
start
```

```python
# have fetch_file_range return times as well as filenames
# then we can assign the timestamp column after dask is finished processing
filenames, date_range = fetch_file_range(start.floor('15T'), end.ceil('15T'), parquet_endpoint, '.parquet')
```

```python
filenames
```

```python
parquet_endpoint
```

```python
start, end
```

```python

# Call your function with appropriate parameters
# Make sure to replace `endpoint`, `start`, `end`, `h_start`, `h_end` with actual values
df = df_from_dask(parquet_endpoint, start, end, h_start, h_end)
```

```python
df = df.compute()
```

```python
df
```

```python
# Get the number of rows (as a delayed object)
num_rows = df.shape[0]

# Get the number of columns (immediately available)
num_columns = len(df.columns)

# To print the shape, you need to compute `num_rows`
num_rows_computed = num_rows.compute()
print(f"Shape of ddf: ({num_rows_computed}, {num_columns})")
```

```python

```
