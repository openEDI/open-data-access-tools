import os
from urllib.parse import urlparse


def format_datetime(dt):
    """Format the datetime string"""
    if not dt:
        return ""

    fmt = "%Y-%m-%d %H:%M:%S"
    return dt.strftime(fmt)


def generate_crawler_name(s3url):
    """Create crawler name by given dataset location.

    Parameters
    ----------
    s3url : str
        The S3 Url string of given dataset location.

    Returns
    -------
    str
        The crawler name.

    Examples
    --------
    >>> generate_crawler_name("s3://bucket-name/Folder1/dataset_name/")
    "bucket-name-folder1-dataset-name"
    """
    bucket, path = parse_s3url(s3url)
    dashed_path = path.replace("/", "-")
    name = f"{bucket}-{dashed_path}".replace("_", "-")
#    name = name[-128:]  # Crawler names have a limit of 128 characters
    return name.lower()


def parse_s3url(s3url):
    """Parse components from a given S3 Url.

    Parameters
    ----------
    url : str
        A S3 Url string.

    Returns
    -------
    tuple
        The (netloc, path) pair.

    Examples
    --------
    >>> parse_url("s3://bucket-name/Folder1/dataset_name/")
    ("bucket-name", "Folder1/dataset_name")
    """
    parse_result = urlparse(s3url)
    bucket = parse_result.netloc
    path = parse_result.path.strip("/")
    return bucket, path


def generate_table_prefix(s3url):
    """Create database table prefix by given dataset location.

    Parameters
    ----------
    s3url : str
        The S3 Url string of given dataset location.

    Returns
    -------
    str
        The crawler name.

    Examples
    --------
    >>> generate_table_prefix("s3://bucket-name/Folder1/dataset_name/")
    "bucket_name_folder1_"

    >>> generate_table_prefix("s3://bucket-name")
    None
    """
    bucket, path = parse_s3url(s3url)

    if not path:
        return None

    if not os.path.dirname(path):
        prefix = f"{bucket}_"
    elif len(path.split("/")) == 1:
        prefix = f"{bucket}_{os.path.dirname(path)}_"
    else:
        prefix = os.path.dirname(path).replace("/", "-") + "_"

    table_prefix = prefix.replace("-", "_").lower()
    table_prefix = table_prefix[-128:]  # A table prefix has a limit of 128 characters
    return table_prefix
