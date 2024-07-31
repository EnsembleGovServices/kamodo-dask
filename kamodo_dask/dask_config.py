from dask.distributed import Client, default_client, LocalCluster, get_worker
import os
import warnings

max_pool_connections = os.environ.get('MAX_POOL_CONNECTIONS', 50)

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


import boto3
from botocore.config import Config
import s3fs

# Define the custom configuration for the boto3 client
boto_config = Config(
    max_pool_connections=max_pool_connections,  # Custom connection pool size
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)

# Create a boto3 client using the custom configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ['ACCESS_KEY'],
    aws_secret_access_key=os.environ['SECRET_KEY'],
    config=boto_config
)



# Create a boto3 session with AWS credentials
boto_session = boto3.Session(
    aws_access_key_id=os.environ['ACCESS_KEY'],
    aws_secret_access_key=os.environ['SECRET_KEY'],
)

# Configuring the S3FileSystem with the custom connection pool size
fs = s3fs.S3FileSystem(
    session=boto_session,  # Pass the session to s3fs
    client_kwargs={
        'config': Config(max_pool_connections=50)  # Increasing the connection pool size
    }
)

# Now pass this S3 filesystem to Dask via storage_options
# Configuration for the S3 connection
storage_options = {
    'key': os.environ['ACCESS_KEY'],
    'secret': os.environ['SECRET_KEY'],
    'config_kwargs': {
        'max_pool_connections': max_pool_connections  # Custom connection pool size
    }
}
