from aws_cdk import core
import boto3

from .data_lake_construct import DataLakeConstruct

class DataLakeStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.oedi_database_name = 'oedi_db'

        crawler_tables = []

        #crawler_tables.append()

        # Create the database
        oedi_data_lake = DataLakeConstruct(
            self,
            'oedi_db',
            database_name=self.oedi_database_name,
            version='0.0.1'

        )

        oedi_data_lake.create_database()

        oedi_data_lake.create_crawler_roles()

        crawlers = []
        crawlers.append(oedi_data_lake.create_crawler('oedi-nrel-garage-array', 's3://oedi-garage-array/'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-nrel-rsf-array', 's3://oedi-rsf-array/'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-nrel-stf-array', 's3://oedi-stf-array/'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-nrel-windsite-array', 's3://oedi-windsite-array/'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-tracking-the-sun-2018', 's3://oedi-dev-tracking-the-sun/2018', 'tracking_the_sun_'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-tracking-the-sun-2019', 's3://oedi-dev-tracking-the-sun/2019', 'tracking_the_sun_'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-pv-rooftops-aspects', 's3://nrel-pv-rooftops/aspects/'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-pv-rooftops-buildings', 's3://nrel-pv-rooftops/buildings/'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-pv-rooftops-developable-planes', 's3://nrel-pv-rooftops/developable_planes/'))
        crawlers.append(oedi_data_lake.create_crawler('oedi-pv-rooftops-rasd', 's3://nrel-pv-rooftops/rasd/'))
