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
from kamodo_dask.kamodo_dask import df_from_dask, KamodoDask, fetch_file_range

import os

parquet_endpoint = os.environ.get('PARQUET_ENDPOINT')

print(parquet_endpoint)

# run this every time you want to fetch new data
start = pd.Timestamp.utcnow() - pd.Timedelta(days=25.5)
# start = pd.Timestamp(2024, 4, 9, 5, 19)

# fetch up to n hours of data
hours_of_data = 2
end = start + pd.Timedelta(seconds=hours_of_data*60*60)

# set range of altitude to fetch
h_start, h_end = 292500.0, 357500.0

# df = df_from_dask(parquet_endpoint, start, end, h_start, h_end)
```

```python
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
parquet_endpoint
```

```python
df = df_from_dask(client, parquet_endpoint, storage_options, start, end, h_start, h_end)
```

```python
df.shape
```

```python
df.head()
```

```python
df
```

Construct a Kamodo object using the retrieved data

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
kd.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(lon=200, lat=0)))
```

```python
kd.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(time=midpoint['time'], h=midpoint['h'])))
```

```python
kd.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(time=midpoint['time'], lon=180)))
```

```python

```
