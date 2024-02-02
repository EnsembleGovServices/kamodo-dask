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

df = df_from_dask(parquet_endpoint, start, end, h_start, h_end)
```

```python

```
