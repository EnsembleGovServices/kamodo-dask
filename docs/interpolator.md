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
# from kamodo_dask.kamodo_dask import df_from_dask, 
from kamodo_dask.kamodo_dask import df_from_dask, KamodoDask

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

Fetch a 4D dataframe using the parquet endpoint

```python
df = df_from_dask(parquet_endpoint, start, end, h_start, h_end)
```

Construct a Kamodo object using the retrieved data

```python
k = KamodoDask(df)
```

```python
k.get_midpoint()
```

```python
k.rho_ijkl(time=1707264300.0, lat=0, lon=0)
```

```python
import plotly.graph_objs as go
from plotly.offline import init_notebook_mode
init_notebook_mode(connected=True)
```

## Plot curve

```python
midpoint = k.get_midpoint()
```

```python
k.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(lon=200, lat=0,  h=midpoint['h'])))
```

## plot slice

```python
k.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(lon=200, lat=0)))
```

```python
k.plot('rho_ijkl', plot_partial=dict(rho_ijkl=dict(time=midpoint['time'], h=midpoint['h'])))
```
