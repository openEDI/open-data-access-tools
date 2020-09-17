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
    assert config.region_name == "us-west-1"


def test_config_datalake_name(config):
    assert config.datalake_name == "my-oedi-datalake"


def test_config_database_name(config):
    assert config.database_name == "my_oedi_database"


def test_config_dataset_locations(config):
    assert config.dataset_locations == [
        "s3://bucket-name/Folder1/dataset_name/",
        "s3://bucket-name/Folder2/another_dataset/"
    ]


def test_config_staging_location(config):
    assert config.staging_location == "s3://my-staging-bucket/"


def test_config_data(config):
    expected = {
        "Region Name": "us-west-1",
        "Datalake Name": "my-oedi-datalake",
        "Database Name": "my_oedi_database",
        "Dataset Locations": [
            "s3://bucket-name/Folder1/dataset_name/",
            "s3://bucket-name/Folder2/another_dataset/"
        ],
        "Staging Location": "s3://my-staging-bucket/"
    }
    assert config.data == expected


def test_config_load(config):
    expected = {
        "AWS": {
            "Region Name": "us-west-1",
            "Datalake Name": "my-oedi-datalake",
            "Database Name": "my_oedi_database",
            "Dataset Locations": [
                "s3://bucket-name/Folder1/dataset_name/",
                "s3://bucket-name/Folder2/another_dataset/"
            ],
            "Staging Location": "s3://my-staging-bucket/"
        },
        "Azure": {
            "A": "something"
        },
        "Google Cloud": {
            "G": "another thing"
        }
    }
    assert config.load() == expected


def test_config_dump(config):
    data = config.load()
    region_name = data["AWS"]["Region Name"]
    assert region_name == "us-west-1"
    
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
    assert template.endswith("s3://my-staging-bucket/\n")
