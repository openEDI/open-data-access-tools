import boto3
from aws_cdk import core

from oedi import __version__
from oedi.config import data_lake_config
from oedi.AWS.data_lake.construct import DataLakeConstruct


class DataLakeStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        oedi_data_lake = DataLakeConstruct(
            scope=self, 
            id="oedi-data-lake-construct", 
            database_name=data_lake_config.database_name, 
            version=__version__
        )

        # Create OEDI database
        oedi_data_lake.create_database()
    
        # Create OEDI crawler role
        oedi_data_lake.create_crawler_role()

        # Create OEDI crawlers 
        for dataset_location in data_lake_config.dataset_locations:
            oedi_data_lake.create_crawler(location=dataset_location)
