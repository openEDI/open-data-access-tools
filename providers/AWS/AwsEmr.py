import boto3
from pprint import pprint

client = boto3.client("emr",'us-west-2')


class AwsEmr():

    def __init__(self, boto3_session):
        self.emr_client = boto3_session.client('emr')

    def lauch_cluster(self, name, launch_bucket, ):

        response = self.emr_client.run_job_flow(
            Name=name,
            LogUri="s3://" + launch_bucket,
            #AdditionalInfo=None,

            ReleaseLabel='emr-5.20.0',
            Instances={
                'MasterInstanceType': 'm4.large',
                'SlaveInstanceType': 'm4.large',
                'InstanceCount': 3,
                'Ec2KeyName': 'nrel-aws-sdi-us-west-2',
                'Ec2SubnetId': 'subnet-a4b769c3',
                'EmrManagedMasterSecurityGroup': 'sg-dc9819a2',
                'EmrManagedSlaveSecurityGroup': 'sg-189b1a66',
                'ServiceAccessSecurityGroup': 'sg-c99b1ab7',
                'KeepJobFlowAliveWhenNoSteps': True
            },

            Applications=[
                {
                    'Name': 'Hadoop'
                },
                {
                    'Name': 'Spark'
                },
                {
                    'Name': 'Presto'
                },
                {
                    'Name': 'JupyterHub'
                },
                {
                    'Name': 'Hive'
                }
            ],
            Configurations=[
                {
                    'Classification': 'spark-hive-site',
                    'Properties': {
                        'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                    }
                },
                {
                    "Classification": "presto-connector-hive",
                    "Properties": {
                        "hive.metastore.glue.datacatalog.enabled": "true"
                    }
                },
                {
                    "Classification": "hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                },
                {
                    "Classification": "emrfs-site",
                    "Properties": {
                        "fs.s3.consistent.retryPeriodSeconds": "10", "fs.s3.consistent": "true",  "fs.s3.consistent.metadata.tableName": "EmrFSMetadata",  "fs.s3.consistent.retryCount": "5"
                    }
                },
                {
                    "Classification": "jupyter-s3-conf",
                    "Properties": {
                        "s3.persistence.enabled": "true",
                        "s3.persistence.bucket": "nrel-jupyterhub"
                    }
                }


            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[
                {
                    'Key': 'org',
                    'Value': 'ops'
                },
            ],
            AutoScalingRole='EMR_AutoScaling_DefaultRole',
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            EbsRootVolumeSize=100

        )