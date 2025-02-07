
from aws_cdk import aws_iam as iam
from aws_cdk import aws_glue as glue
from constructs import Construct

from oedi.AWS.utils import generate_crawler_name, generate_table_prefix


class AWSDataLakeConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        account: object,
        database_name: str,
        version: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        self._account = account
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
        glue.CfnDatabase(
            scope=self,
            id=f"oedi-data-lake-database-{self.database_name}",
            catalog_id=self._account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.database_name
            )
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

    def create_crawler(self, location, table_prefix, tags):
        """Create crawler in data lake by given dataset location."""
        crawler_name = generate_crawler_name(s3url=location)

        if not self.crawler_role:
            self.crawler_role()

        glue.CfnCrawler(
            scope=self,
            id=f"oedi-data-lake-crawler--{crawler_name}",
            name=crawler_name,
            role=self.crawler_role.role_name,
            targets={"s3Targets": [{"path": location}]},
            database_name=self.database_name,
            table_prefix=table_prefix,
            tags=tags
        )
