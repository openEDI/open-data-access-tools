"""
Utility functions used for constructing data lake.
"""
import os
from urllib.parse import urlparse
from operator import itemgetter

import pandas as pd
import yaml
import boto3
from botocore.exceptions import ClientError

from oedi.config import data_lake_config
from oedi.AWS.base import AWSClientBase


class OEDIGlue(AWSClientBase):

    def __init__(self, **kwargs):
        super().__init__(service_name="glue", **kwargs)

    def get_databases(self):
        response = self.client.get_databases()
        databases = [
            {
                "Name": db["Name"], 
                "CreateTime": format_datetime(db["CreateTime"])
            }
            for db in response["DatabaseList"]
        ]
        return databases

    def list_tables(self, database_name):
        """List avaible tables in given database"""
        paginator = self.client.get_paginator("get_tables")
        
        tables = []
        for response in paginator.paginate(DatabaseName=database_name):
            for tb in response["TableList"]:
                tables.append({
                    "Name": f"{tb['Name']}",
                    "CreateTime": tb["CreateTime"]
                })

        return sorted(tables, key=itemgetter("Name"))

    def get_table(self, database_name, table_name):
        """Get given table detail information"""
        response = self.client.get_table(DatabaseName=database_name, Name=table_name)
        return response["Table"]

    def get_table_columns(self, database_name, table_name, with_pandas=True):
        """Get table columns"""
        table = self.get_table(database_name, table_name)
        columns = table["StorageDescriptor"]["Columns"]
        if with_pandas:
            columns = pd.DataFrame(columns)
        return columns

    def get_partition_keys(self, database_name, table_name, with_pandas=True):
        """Get table partition keys"""
        table = self.get_table(database_name, table_name)
        partition_keys = table["PartitionKeys"]
        if with_pandas:
            partition_keys = pd.DataFrame(partition_keys)
        return partition_keys

    def get_partition_values(self, database_name, table_name):
        """Get given table partition values"""
        partition_values = []
        paginator = self.client.get_paginator("get_partitions")
        for response in paginator.paginate(DatabaseName=database_name, TableName=table_name):
            partitions = response["Partitions"]
            for partition in partitions:
                partition_values.extend(partition["Values"])
        return partition_values

    def list_crawlers(self):
        """List available crawlers"""
        # Crawlers definited in OEDI datalake.
        oedi_crawler_names = set([
            generate_crawler_name(s3url=dataset_location)
            for dataset_location in data_lake_config.dataset_locations
        ])

        # Access to each crawler details
        available_crawlers = []
        for crawler_name in oedi_crawler_names:
            crawler = self.client.get_crawler(Name=crawler_name)["Crawler"]
            available_crawlers.append({
                "Name": crawler["Name"],
                "State": crawler["State"],
                "Role": crawler["Role"],
                "S3Targets": crawler["Targets"]["S3Targets"][0]["Path"],
                "LastUpdated": crawler["LastUpdated"],
                "CreateTime": crawler["CreationTime"]
            })

        return sorted(available_crawlers, key=itemgetter("Name"))

    def get_crawler_state(self, crawler_name):
        """
        Check the crawler state by given crawler name
        State: READY | RUNNING | STOPPING
        """
        crawler = self.client.get_crawler(Name=crawler_name)
        if "LastCrawl" not in crawler["Crawler"]:  # For new crawler
            state = "READY"
        else:
            state = crawler["Crawler"]["State"]
        
        return state

    def start_crawler(self, crawler_name):
        """Run crawler by given crawler name"""
        try:
            self.client.start_crawler(Name=crawler_name)
        except ClientError as e:
            print(f"StartCrawlerError: {str(e)}")


def format_datetime(dt):
    """Format the datetime string"""
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
    else:
        prefix = f"{bucket}_{os.path.dirname(path)}_"

    table_prefix = prefix.replace("-", "_")
    return table_prefix.lower()
