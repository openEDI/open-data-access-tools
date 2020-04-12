"""
Utility functions used for constructing data lake.
"""
import os
from urllib.parse import urlparse

import yaml
import boto3
from botocore.exceptions import ClientError

from oedi.config import data_lake_config


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


def create_crawler_name(s3url):
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
    >>> create_crawler_name("s3://bucket-name/Folder1/dataset_name/")
    "bucket-name-folder1-dataset-name"
    
    """
    bucket, path = parse_s3url(s3url)
    dashed_path = path.replace("/", "-")
    name = f"{bucket}-{dashed_path}".replace("_", "-")
    return name.lower()


def create_table_prefix(s3url):
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
    >>> create_table_prefix("s3://bucket-name/Folder1/dataset_name/")
    "bucket_name_folder1_"
    
    >>> create_table_prefix("s3://bucket-name")
    None
    
    """
    bucket, path = parse_s3url(s3url)
    
    if not path:
        return None
    
    if not os.path.dirname(path):
        prefix = f"{bucket}_"
    else:
        prefix = f"{bucket}_{os.path.dirname(path)}_"

    table_prefix = prefix.replace("-", "_")
    return table_prefix.lower()


def list_available_crawlers():
    """List available crawlers"""
    client = boto3.client("glue", region_name=data_lake_config.aws_region)
    
    try:
        all_crawlers = set(client.list_crawlers()["CrawlerNames"])
    except ClientError as e:
        all_crawlers = []

    potential_crawlers = set([
        create_crawler_name(s3url=dataset_location)
        for dataset_location in data_lake_config.dataset_locations
    ])
    
    available_crawlers = list(all_crawlers.intersection(potential_crawlers))
    return sorted(available_crawlers)


def start_crawler(crawler_name):
    """Run crawler by given crawler name"""
    client = boto3.client("glue", region_name=data_lake_config.aws_region)
    
    try:
        client.start_crawler(Name=crawler_name)
    except ClientError as e:
        print(f"StartCrawlerError: {str(e)}")


def check_crawler_state(crawler_name):
    """
    Check the crawler state by given crawler name
    State: READY | RUNNING | STOPPING
    """
    client = boto3.client("glue", region_name=data_lake_config.aws_region)
    crawler = client.get_crawler(Name=crawler_name)

    if "LastCrawl" not in crawler["Crawler"]:  # For new crawler
        state = "READY"
    else:
        state = crawler["Crawler"]["State"]
    
    return state


def list_available_tables(database_name):
    """List avaible tables in given database"""
    client = boto3.client("glue", region_name=data_lake_config.aws_region)
    
    paginator = client.get_paginator("get_tables")
    response_iterator = paginator.paginate(DatabaseName=database_name)
    
    tables = []
    for response in response_iterator:
        for tb in response["TableList"]:
            tables.append(f"{database_name}.{tb['Name']}")
    
    return sorted(tables)
