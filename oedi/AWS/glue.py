"""
Utility functions used for constructing data lake.
"""
from operator import itemgetter

import pandas as pd
from botocore.exceptions import ClientError

from oedi.config import AWSDataLakeConfig, OEDI_CONFIG_FILE
from oedi.AWS.base import AWSClientBase
from oedi.AWS.utils import generate_crawler_name, format_datetime


class OEDIGlue(AWSClientBase):

    def __init__(self, config_file=None, **kwargs):
        super().__init__(service_name="glue", **kwargs)
        self.config_file = config_file or OEDI_CONFIG_FILE

    def get_databases(self):
        response = self.client.get_databases()
        databases = [
            {
                "Name": db["Name"], 
                "CreateTime": format_datetime(db.get("CreateTime", None))
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
                    "CreateTime": format_datetime(tb.get("CreateTime", None))
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
        data_lake_config = AWSDataLakeConfig(self.config_file)
        oedi_crawler_names = set([
            generate_crawler_name(s3url=dataset_location)
            for dataset_location in data_lake_config.dataset_locations
        ])

        # Access to each crawler details
        available_crawlers = []
        for crawler_name in oedi_crawler_names:
            crawler = self.get_crawler(crawler_name)["Crawler"]
            available_crawlers.append({
                "Name": crawler["Name"],
                "State": crawler["State"],
                "Role": crawler["Role"],
                "S3Targets": crawler["Targets"]["S3Targets"][0]["Path"],
                "LastUpdated": crawler["LastUpdated"],
                "CreateTime": crawler["CreationTime"]
            })

        return sorted(available_crawlers, key=itemgetter("Name"))

    def get_crawler(self, crawler_name):
        """Get the crawler from Glue"""
        return self.client.get_crawler(Name=crawler_name)

    def get_crawler_state(self, crawler_name):
        """
        Check the crawler state by given crawler name
        State: READY | RUNNING | STOPPING
        """
        crawler = self.get_crawler(crawler_name)
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
