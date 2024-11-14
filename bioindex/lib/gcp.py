from google.auth import default
from googleapiclient.discovery import build
from google.cloud import secretmanager

import sqlalchemy.engine

PROJECT_ID = 'aaa-willyn-test'

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
    return ""


def get_bgzip_job_status(job_id: str):
    return None


def invoke_lambda(function_name, payload):
    """
    Invokes an AWS lambda function and waits for it to complete.
    """
    return ''


def start_and_wait_for_indexer_job(file: str, index: str, arity: int, bucket: str, rds_secret: str, rds_schema: str,
                                   size: int):
    return {}


def connect_to_db(schema=None, **kwargs):
    """
    Connect to a MySQL database using keyword arguments.
    """
    if not schema:
        schema = kwargs.get('dbname')

    # build the connection uri
    #uri = '{engine}+pymysql://{username}:{password}@{host}/{schema}?local_infile=1'.format(schema=schema, **kwargs)
    uri = 'mysql+pymysql://{username}:{password}@127.0.0.1:3306/{schema}?local_infile=1'.format(schema=schema, **kwargs)

    # create the connection pool
    engine = sqlalchemy.create_engine(uri, pool_recycle=3600)

    # test the engine by making a single connection
    with engine.connect():
        return engine


def get_project_id() -> str:
    """
    Retrieve project id from environment or metadata server.
    """
    # In python 3.7, this works for App Engine
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

    if not project_id:
        # In python 3.7, this works for Cloud Run
        project_id = os.getenv("GCP_PROJECT")

    if not project_id:  # > python37, use Metdata Server
        # Only works on runtime.
        import urllib.request

        url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
        req = urllib.request.Request(url)
        req.add_header("Metadata-Flavor", "Google")
        project_id = urllib.request.urlopen(req).read().decode()

    if not project_id:  # Running locally
        with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], "r") as fp:
            credentials = json.load(fp)
        project_id = credentials["project_id"]

    if not project_id:
        raise ValueError("Could not get a value for PROJECT_ID")

    return project_id


def access_secret(secret_id: str) -> str:
    """
    Access secret value in Secret Manager.
    Example secret_id: dig-bioindex/versions/1
    """
    PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT',None)
    PROJECT_PREFIX = f'projects/{PROJECT_ID}/secrets/'

    # Create the Secret Manager client.
    try:
        client = secretmanager.SecretManagerServiceClient()
    except:
        print('Did not create SecretManagerClient')

    # Access the secret version.
    response = client.access_secret_version(
        request={'name': PROJECT_PREFIX+secret_id})

    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode('UTF-8')
    # print("Plaintext: {}".format(payload))
    return payload
