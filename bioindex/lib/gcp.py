from google.auth import default
from googleapiclient.discovery import build
from google.cloud import storage

import sqlalchemy.engine

PROJECT_ID = 'broad-tools-development'

gcs_client = storage.Client()

# Initialize the Cloud SQL Admin client using default credentials
def get_sqladmin_client():
    credentials, project = default()  # Automatically fetches ADC
    return build('sqladmin', 'v1beta4', credentials=credentials)

def describe_cloudsql_instance(instance_name):
    """
    Returns a dictionary with the engine, host, and port information
    for the requested Cloud SQL instance.
    """
    sqladmin_client = get_sqladmin_client()
    request = sqladmin_client.instances().get(project=PROJECT_ID, instance=instance_name)
    instance = request.execute()

    if not instance:
        raise RuntimeError('No Cloud SQL instance found with the specified name')

    # Map Cloud SQL instance information to match the RDS response structure
    return {
        'name': instance_name,
        'engine': instance['databaseVersion'],
        'host': instance['ipAddresses'][0]['ipAddress'],  # Primary IP
        'port': 5432 if 'POSTGRES' in instance['databaseVersion'] else 3306,  # Default ports
        'dbname': instance.get('settings', {}).get('userLabels', {}).get('dbname')
    }


def start_batch_job(index_name: str, s3_path: str, job_definition: str, additional_parameters: dict = None):
    raise NotImplementedError("start_batch_job still todo")
    return ""


def get_bgzip_job_status(job_id: str):
    raise NotImplementedError("get_bgzip_job_status still todo")
    return None


def invoke_lambda(function_name, payload):
    """
    Invokes an AWS lambda function and waits for it to complete.
    """
    raise NotImplementedError("invoke_lambda still todo")
    return ''


def start_and_wait_for_indexer_job(file: str, index: str, arity: int, bucket: str, rds_secret: str, rds_schema: str,
                                   size: int):
    raise NotImplementedError("start_and_wait_for_indexer_job still todo")
    return {}


def connect_to_db(schema=None, **kwargs):
    """
    Connect to a MySQL database using keyword arguments.
    """
    if not schema:
        schema = kwargs.get('dbname')

    # build the connection uri
    #uri = '{engine}+pymysql://{username}:{password}@{host}/{schema}?local_infile=1'.format(schema=schema, **kwargs)
    # Using a CloudSQL proxy for now.
    #uri = 'mysql+pymysql://{username}:{password}@127.0.0.1:3306/{schema}?local_infile=1'.format(schema=schema, **kwargs)
    uri = 'mysql+pymysql://{username}:{password}@/{schema}?unix_socket=/cloudsql/{INSTANCE_CONNECTION_NAME}'.format(
        schema=schema,
        INSTANCE_CONNECTION_NAME='broad-tools-development:us-central1:dig-bioindex-dev',
        **kwargs
    )

    # create the connection pool
    engine = sqlalchemy.create_engine(uri, pool_recycle=3600)

    # test the engine by making a single connection
    with engine.connect():
        return engine