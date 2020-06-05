from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_athena as athena,
    core as core,
)

from oedi.AWS.utils.glue import generate_crawler_name, generate_table_prefix


class DataLakeConstruct(core.Construct):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        database_name: str,
        # read_write_iam_users: list,
        # read_only_iam_users: list,
        version: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        self._database_name = database_name
        self._crawler_role = None

    @property
    def database_name(self):
        """The database name of data lake"""
        return self._database_name

    @property
    def crawler_role(self):
        """The role used by crawlers in data lake"""
        return self._crawler_role

    def create_database(self):
        """Create the database of data lake in Glue"""
        id_suffix = self.database_name.replace("_", "-")
        glue.Database(
            scope=self, 
            id=f"oedi-data-lake-database--{id_suffix}", 
            database_name=self.database_name
        )

    def create_crawler_role(self):
        """Crate a role used by crawlers in data lake."""
        service = iam.ServicePrincipal("glue.amazonaws.com")
        managed_policies = [
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            ),
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonS3ReadOnlyAccess"
            ),
        ]
        
        id_suffix = self.database_name.replace("_", "-")
        self._crawler_role = iam.Role(
            scope=self,
            id=f"oedi-data-lake-crawler-role--{id_suffix}",
            role_name=f"oedi_data_lake_cralwer_role__{self.database_name}",
            assumed_by=service,
            managed_policies=managed_policies,
        )

    def create_crawler(self, location):
        """Create crawler in data lake by given dataset location."""
        crawler_name = generate_crawler_name(s3url=location)
        table_prefix = generate_table_prefix(s3url=location)

        if not self.crawler_role:
            self.crawler_role()

        crawler = glue.CfnCrawler(
            scope=self,
            id=f"oedi-data-lake-crawler--{crawler_name}",
            name=crawler_name,
            role=self.crawler_role.role_name,
            targets={"s3Targets": [{"path": location}]},
            database_name=self.database_name,
            table_prefix=table_prefix,
        )
