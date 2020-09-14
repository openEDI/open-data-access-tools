import boto3
from aws_cdk import core

from oedi import __version__
from oedi.config import OEDI_CONFIG_FILE, AWSDataLakeConfig
from oedi.AWS.data_lake.construct import AWSDataLakeConstruct


class AWSDataLakeStack(core.Stack):
    """AWS data lake stack class"""
    
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        """Lauch AWS data lake related infrastructures."""
        super().__init__(scope, id, **kwargs)
        
        config = AWSDataLakeConfig(OEDI_CONFIG_FILE)
        data_lake = AWSDataLakeConstruct(
            scope=self,
            id="oedi-data-lake-construct",
            database_name=config.database_name, 
            version=__version__
        )
        data_lake.create_database()
        data_lake.create_crawler_role()
        for dataset_location in config.dataset_locations:
            data_lake.create_crawler(location=dataset_location)
