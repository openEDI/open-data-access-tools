from aws_cdk import (
     aws_iam as iam,
     aws_s3 as s3,
     aws_glue as glue,
     aws_athena as athena,
     core as core
)

class DataLakeConstruct(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
                 database_name: str,
                 #read_write_iam_users: list,
                 #read_only_iam_users: list,
                 version: str
                 ) -> None:

        super().__init__(scope, id)

        self._database_name = database_name
        self.garage_array_bucket = 's3://somebucket'

    def create_database(self):

        database = glue.Database(self,
                                 "glue-db",
                                 database_name=self._database_name)
    def create_crawler_roles(self):

        service = iam.ServicePrincipal(
            'glue.amazonaws.com'
        )

        self.crawler_role = iam.Role(
            self,
            f'oedi_{self._database_name}_cralwer_role',
            role_name = f'oedi_{self._database_name}_cralwer_role',
            assumed_by=service,
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                              iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess")]
        )



    def create_crawler(self, name, location, table_prefix=None):

        crawler = glue.CfnCrawler(
                                  self,
                                  name,
                                  name=name,
                                  role=self.crawler_role.role_name,
                                  targets={
                                        "s3Targets": [{"path": location}]
                                  },
                                  database_name=self._database_name,
                                  table_prefix=table_prefix
        )
