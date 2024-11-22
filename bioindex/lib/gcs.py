from io import BytesIO
import gzip
import os
import re
import fnmatch

from .gcp import gcs_client

def list_objects(bucket_name, prefix, only=None, exclude=None, max_keys=None):
    """
    Generator function that returns all the objects in GCS with a given prefix.
    If the prefix is an absolute path (beginning with "gs://" then the bucket
    of the URI is used instead.
    """
    # Get the GCS bucket
    bucket = gcs_client.get_bucket(bucket_name)
    
    # Set up options for listing blobs
    blobs = bucket.list_blobs(prefix=prefix.strip('/') + '/')
    count = 0

    for blob in blobs:
        if max_keys and count >= max_keys:
            break

        path = blob.name
        file = os.path.basename(path)

        # Ignore empty files
        if blob.size == 0:
            continue

        # Apply `only` and `exclude` filters
        if only and not fnmatch.fnmatch(file, only):
            continue
        if exclude and fnmatch.fnmatch(file, exclude):
            continue

        # Yield blob metadata
        yield {
            'Key': blob.name,
            'Size': blob.size,
            'LastModified': blob.updated,
            'ETag': blob.etag
        }

        count += 1


def read_object(bucket_name, path, offset=None, length=None):
    """
    Open a gcs object and return a streaming portion of it. If the path is
    an "absolute" path (begins with "gs://") then the bucket name is overridden
    and the bucket from the path is used.
    """
    # Access the bucket and blob (object) in GCS
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(path)

    # Specify the range for reading
    if offset is not None or length is not None:
        # Fetch bytes as specified by offset and length
        end = offset + length - 1 if offset is not None and length is not None else None
        data = blob.download_as_bytes(start=offset, end=end)
    else:
        # Fetch the entire object
        data = blob.download_as_bytes()

    return data

def read_lined_object(bucket, path, offset=None, length=None):
    raw = read_object(bucket, path, offset, length)
    if path.endswith('.gz'):
        bytestream = BytesIO(raw)
        gzip_file = gzip.open(bytestream, 'rt')
        return (line.rstrip("\n") for line in gzip_file)  # This is a generator expression, not a tuple.
    else:
        return (line.decode('utf-8').rstrip("\n") for line in raw.iter_lines())


def relative_key(key, common_prefix, strip_uuid=True):
    """
    Given an S3 key like:

      foo/bar/baz/part-00015-59b75a7e-56ef-4183-bf26-48f67c6f33c7-c000.json

    And a common prefix for the key like:

      foo/bar/

    This should simplify and return: baz/part-00015.json
    """
    simple_key = key

    if simple_key.startswith(common_prefix):
        simple_key = simple_key[len(common_prefix):]

    if strip_uuid:
        simple_key = re.sub(r'(?:-[0-9a-f]+){6}(?=\.)', '', simple_key, re.IGNORECASE)

    return simple_key