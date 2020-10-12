from aws_cdk import core

from oedi.config import AWSDataLakeConfig
from oedi.AWS.data_lake.stack import AWSDataLakeStack
from tests.unit.test_config import OEDI_TEST_CONFIG_FILE


def test_data_lake():
    app = core.App()
    config = AWSDataLakeConfig(OEDI_TEST_CONFIG_FILE)

    AWSDataLakeStack(app, config)
    cloud_assembly = app.synth()
    cfn = cloud_assembly.stacks[0]

    # naming
    datalake_name = "my-oedi-datalake"
    assert cfn.id == datalake_name
    assert cfn.stack_name == datalake_name
    assert cfn.name == datalake_name
    assert cfn.display_name == datalake_name
    assert cfn.original_name == datalake_name

    # My data lake settings
    assert len(cfn.template["Resources"]) == 4

    expected_database_name = "my_oedi_database"
    expected_role_name = "oedi_data_lake_cralwer_role__my_oedi_database"
    expected_cralwer_names = [
        "bucket-name-folder1-dataset-name",
        "bucket-name-folder2-another-dataset"
    ]

    for key, res in cfn.template["Resources"].items():
        if res["Type"] == "AWS::Glue::Database":
            assert res["Properties"]["DatabaseInput"]["Name"] == expected_database_name

        if res["Type"] == "AWS::Glue::Role":
            assert res["Properties"]["RoleName"] == expected_role_name

        if res["Type"] == "AWS::Glue::Crawler":
            crawler_name = res["Properties"]["Name"]
            assert crawler_name in expected_cralwer_names
