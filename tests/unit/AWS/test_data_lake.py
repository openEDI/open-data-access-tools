from aws_cdk import App

from oedi.config import AWSDataLakeConfig
from oedi.AWS.data_lake.stack import AWSDataLakeStack
from tests.unit.test_config import OEDI_TEST_CONFIG_FILE


def test_data_lake():
    app = App()
    config = AWSDataLakeConfig(OEDI_TEST_CONFIG_FILE)

    AWSDataLakeStack(app, config)
    cloud_assembly = app.synth()
    cfn = cloud_assembly.stacks[0]

    # naming
    datalake_name = "aws-oedi-datalake"
    assert cfn.id == datalake_name
    assert cfn.stack_name == datalake_name
    assert cfn.name == datalake_name
    assert cfn.display_name == datalake_name
    assert cfn.original_name == datalake_name

    # My data lake settings
    assert len(cfn.template["Resources"]) == 9

    database_names = set()
    role_names = set()
    crawler_names = set()
    for key, res in cfn.template["Resources"].items():
        if res["Type"] == "AWS::Glue::Database":
            database_names.add(res["Properties"]["DatabaseInput"]["Name"])

        if res["Type"] == "AWS::IAM::Role":
            role_names.add(res["Properties"]["RoleName"])

        if res["Type"] == "AWS::Glue::Crawler":
            crawler_names.add(res["Properties"]["Name"])
    
    assert database_names == set([
        "oedi_data_one",
        "oedi_data_two"
    ])
    assert role_names == set([
        "oedi_data_lake_cralwer_role__oedi_data_one",
        "oedi_data_lake_cralwer_role__oedi_data_two"
    ])
    assert crawler_names == set([
        "bucket-name-data-one-part-a",
        "bucket-name-data-one-part-b",
        "bucket-name-data-two-2015",
        "bucket-name-data-two-2016",
        "bucket-name-data-three-2017"
    ])
