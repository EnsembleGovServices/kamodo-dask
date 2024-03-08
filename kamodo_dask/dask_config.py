from dask.distributed import Client, default_client, LocalCluster, get_worker
import os
import warnings

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