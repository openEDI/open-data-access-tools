from aws_cdk import core

from oedi import __version__
from oedi.config import OEDIConfigBase
from oedi.AWS.data_lake.construct import AWSDataLakeConstruct


class AWSDataLakeStack(core.Stack):
    """AWS data lake stack class"""

    def __init__(self, scope: core.Construct, config: OEDIConfigBase) -> None:
        """Lauch AWS data lake related infrastructures."""
        super().__init__(scope, config.datalake_name, env={"region": config.region_name})

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
            
        data_lake_buildstock = AWSDataLakeConstruct(
            scope=self,
            id="oedi-data-lake-construct-buildstock",
            database_name=config.buildstock_database_name,
            version=__version__
        )
        data_lake_buildstock.create_database()
        data_lake_buildstock.create_crawler_role()
        for dataset_location in config.buildstock_dataset_locations:
            data_lake_buildstock.create_crawler(location=dataset_location)
