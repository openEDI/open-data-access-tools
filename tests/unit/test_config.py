import os
import pytest
from oedi.config import AWSDataLakeConfig

OEDI_TEST_CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test_config.yaml"
)


@pytest.fixture
def config():
    return AWSDataLakeConfig(OEDI_TEST_CONFIG_FILE)


def test_config_provider(config):
    assert config.provider == "AWS"


def test_config_file(config):
    assert config.config_file == OEDI_TEST_CONFIG_FILE


def test_config_region_name(config):
    assert config.region_name == "us-west-2"


def test_config_datalake_name(config):
    assert config.datalake_name == "aws-oedi-datalake"


def test_config_databases(config):
    assert len(config.databases) == 2
    for database in config.databases:
        assert "Identifier" in database
        assert "Name" in database
        assert "Locations" in database
        
        if database["Name"] == "data-one":
            assert len(database["Locations"]) == 2
        
        if database["Name"] == "data-two":
            assert len(database["Locations"]) == 3


def test_config_staging_location(config):
    assert config.staging_location == "s3://aws-staging-bucket/"


def test_config_data(config):
    expected = {
        "Region Name": "us-west-2",
        "Datalake Name": "aws-oedi-datalake",
        "Staging Location": "s3://aws-staging-bucket/",
        "Databases": [
            {
                "Identifier": "data-one",
                "Name": "oedi-data-one",
                "Locations": [
                    "s3://bucket-name/data-one/part-a/",
                    "s3://bucket-name/data-one/part-b/"
                ]
            },
            {
                "Identifier": "data-two",
                "Name": "oedi-data-two",
                "Locations": [
                    "s3://bucket-name/data-two/2015/",
                    "s3://bucket-name/data-two/2016/",
                    "s3://bucket-name/data-three/2017/"
                ]
            }
        ]
    }
    assert config.data == expected


def test_config_load(config):
    expected = {
        "AWS": {
            "Region Name": "us-west-2",
            "Datalake Name": "aws-oedi-datalake",
            "Staging Location": "s3://aws-staging-bucket/",
            "Databases": [
                {
                    "Identifier": "data-one",
                    "Name": "oedi-data-one",
                    "Locations": [
                        "s3://bucket-name/data-one/part-a/",
                        "s3://bucket-name/data-one/part-b/"
                    ]
                },
                {
                    "Identifier": "data-two",
                    "Name": "oedi-data-two",
                    "Locations": [
                        "s3://bucket-name/data-two/2015/",
                        "s3://bucket-name/data-two/2016/",
                        "s3://bucket-name/data-three/2017/"
                    ]
                }
            ]
        },
        "Azure": {
            "Datalake Name": "azure-oedi-datalake"
        },
        "Google": {
            "Datalake Name": "google-oedi-datalake"
        }
    }
    assert config.load() == expected


def test_config_dump(config):
    data = config.load()
    region_name = data["AWS"]["Region Name"]
    assert region_name == "us-west-2"

    # Update
    new_region_name = "us-east-1"
    data["AWS"]["Region Name"] = new_region_name
    config.dump(data)

    data = config.load()
    assert data["AWS"]["Region Name"] == new_region_name

    # Revert back
    data["AWS"]["Region Name"] = region_name
    config.dump(data)


def test_config_to_string(config):
    template = config.to_string()
    assert template.endswith("s3://aws-staging-bucket/\n")
