FROM  kamodo/dask

COPY dev-requirements.txt /code
RUN pip install -r dev-requirements.txt

