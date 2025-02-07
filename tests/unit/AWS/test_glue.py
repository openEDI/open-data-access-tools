import copy
import boto3
import mock
from moto import mock_aws

from oedi.AWS.glue import OEDIGlue
from tests.unit.test_config import OEDI_TEST_CONFIG_FILE


TABLE_INPUT = {
    "Owner": "oedi",
    "Description": "Fake table for test.",
    "Retention": 3,
    "StorageDescriptor": {
        "BucketColumns": [],
        "Compressed": False,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "NumberOfBuckets": -1,
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "Parameters": {},
        "SerdeInfo": {
            "Parameters": {"serialization.format": "1"},
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        },
        "SkewedInfo": {
            "SkewedColumnNames": [],
            "SkewedColumnValueLocationMaps": {},
            "SkewedColumnValues": [],
        },
        "SortColumns": [],
        "StoredAsSubDirectories": False,
    },
    "Parameters": {"EXTERNAL": "TRUE"},
    "TableType": "EXTERNAL_TABLE",
    "PartitionKeys": []
}

CRAWLERS = [
    {
        "Name": "bucket-name-data-one-part-a",
        "State": "READY",
        "Role": "my-glue-role",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "s3://bucket-name/data-one/part-a/"
                }
            ]
        },
        "LastUpdated": "2020-02-05 12:00:01",
        "CreationTime": "2019-11-12 13:01:03"
    },
    {
        "Name": "bucket-name-data-one-part-b",
        "State": "READY",
        "Role": "my-glue-role",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "s3://bucket-name/data-one/part-b/"
                }
            ]
        },
        "LastUpdated": "2020-03-01 10:00:02",
        "CreationTime": "2019-10-02 09:01:04"
    },
    {
        "Name": "bucket-name-data-two-2015",
        "State": "READY",
        "Role": "my-glue-role",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "s3://bucket-name/data-one/part-a/"
                }
            ]
        },
        "LastUpdated": "2020-02-05 12:00:03",
        "CreationTime": "2019-11-12 13:01:05"
    },
    {
        "Name": "bucket-name-data-two-2016",
        "State": "READY",
        "Role": "my-glue-role",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "s3://bucket-name/data-two/2016/"
                }
            ]
        },
        "LastUpdated": "2020-03-01 10:00:04",
        "CreationTime": "2019-10-02 09:01:06"
    },
    {
        "Name": "bucket-name-data-three-2017",
        "State": "READY",
        "Role": "my-glue-role",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "s3://bucket-name/data-three/2017/"
                }
            ]
        },
        "LastUpdated": "2020-03-01 10:00:05",
        "CreationTime": "2019-10-02 09:01:07"
    }
]


@mock_aws
def test_oedi_glue__get_databasses():
    # Setup
    client = boto3.client("glue", region_name="us-west-1")

    database_name = "test-database"
    client.create_database(DatabaseInput={"Name": database_name})

    # Test
    glue = OEDIGlue()
    glue._client = client
    response = glue.get_databases()
    assert len(response) == 1

    db = response[0]
    assert db["Name"] == database_name
    assert db["CreateTime"] is not None


def create_table_input(
        database_name,
        table_name,
        partition_keys=None,
        columns=None):
    """A helper function for creating table input."""
    table_input = copy.deepcopy(TABLE_INPUT)
    table_input["Name"] = table_name
    table_input["PartitionKeys"] = partition_keys or []
    table_input["StorageDescriptor"]["Columns"] = columns or []

    location = f"s3://test-bucket/{database_name}/{table_name}"
    table_input["StorageDescriptor"]["Location"] = location
    return table_input


@mock_aws
def test_oedi_glue__get_table():
    # Setup
    client = boto3.client("glue", region_name="us-west-1")

    database_name = "test-database"
    client.create_database(DatabaseInput={"Name": database_name})

    table_name = "test-table"
    partition_keys = [{"Name": "state"}, {"Name": "city"}]
    table_input = create_table_input(database_name, table_name, partition_keys)
    client.create_table(DatabaseName=database_name, TableInput=table_input)

    # Test
    glue = OEDIGlue()
    glue._client = client
    table = glue.get_table(database_name, table_name)
    assert table["DatabaseName"] == "test-database"
    assert table["Name"] == "test-table"
    assert table["PartitionKeys"] == partition_keys


@mock_aws
def test_oedi_glue__list_tables():
    # Setup
    client = boto3.client("glue", region_name="us-west-1")

    database_name = "test-database"
    client.create_database(DatabaseInput={"Name": database_name})

    table_name = "test-table-1"
    table_input = create_table_input(database_name, table_name)
    client.create_table(DatabaseName=database_name, TableInput=table_input)

    table_name = "test-table-2"
    table_input = create_table_input(database_name, table_name)
    client.create_table(DatabaseName=database_name, TableInput=table_input)

    # Test
    glue = OEDIGlue()
    glue._client = client
    tables = glue.list_tables(database_name)
    assert len(tables) == 2
    assert "CreateTime" in tables[0]


@mock_aws
def test_oedi_glue__get_table_columns():
    # Setup
    client = boto3.client("glue", region_name="us-west-1")

    database_name = "test-database"
    client.create_database(DatabaseInput={"Name": database_name})

    table_name = "test-table"
    mycolumns = [
        {"Name": "state", "Type": "string"},
        {"Name": "size", "Type": "integer"}
    ]
    table_input = create_table_input(database_name, table_name, columns=mycolumns)
    client.create_table(DatabaseName=database_name, TableInput=table_input)

    # Test
    glue = OEDIGlue()
    glue._client = client
    columns = glue.get_table_columns(database_name, table_name)
    assert len(columns) == 2


@mock_aws
def test_oedi_glue__get_partition_keys():
    # Setup
    client = boto3.client("glue", region_name="us-west-1")

    database_name = "test-database"
    client.create_database(DatabaseInput={"Name": database_name})

    table_name = "test-table"
    my_partition_keys = [
        {"Name": "state"},
        {"Name": "city"},
        {"Name": "zipcode"}
    ]
    table_input = create_table_input(database_name, table_name, my_partition_keys)
    client.create_table(DatabaseName=database_name, TableInput=table_input)

    # Test
    glue = OEDIGlue()
    glue._client = client
    partition_keys = glue.get_partition_keys(database_name, table_name)
    assert len(partition_keys) == 3


@mock_aws
def test_oedi_glue__get_partition_values():
    # Setup
    client = boto3.client("glue", region_name="us-west-1")

    database_name = "test-database"
    client.create_database(DatabaseInput={"Name": database_name})

    table_name = "test-table"
    my_partition_keys = [
        {"Name": "state"},
        {"Name": "city"},
        {"Name": "zipcode"}
    ]
    table_input = create_table_input(database_name, table_name, my_partition_keys)
    client.create_table(DatabaseName=database_name, TableInput=table_input)

    # Test
    glue = OEDIGlue()
    glue._client = client
    values = glue.get_partition_values(database_name, table_name)
    assert len(values) == 0


def get_crawler(crawler_name):
    """A side effect for getting crawler."""
    for crawler in CRAWLERS:
        if crawler["Name"] != crawler_name:
            continue
        return {"Crawler": crawler}
    raise Exception(f"Crawler '{crawler_name}' does not exist.")


@mock_aws
@mock.patch("oedi.AWS.glue.OEDIGlue.get_crawler", side_effect=get_crawler)
def test_oedi_glue__get_crawler(mock_get_crawler):
    glue = OEDIGlue()
    crawler_name = "bucket-name-data-one-part-a"
    crawler = glue.get_crawler(crawler_name)["Crawler"]
    assert crawler["Name"] == crawler_name


@mock_aws
@mock.patch("oedi.AWS.glue.OEDIGlue.get_crawler", side_effect=get_crawler)
def test_oedi_glue__list_crawlers(mock_get_crawler):
    glue = OEDIGlue(config_file=OEDI_TEST_CONFIG_FILE)
    crawlers = glue.list_crawlers()
    assert len(crawlers) == 5


@mock_aws
@mock.patch("oedi.AWS.glue.OEDIGlue.get_crawler", side_effect=get_crawler)
def test_oedi_glue__get_crawler_state(mock_get_crawler):
    glue = OEDIGlue(config_file=OEDI_TEST_CONFIG_FILE)
    crawlers = glue.list_crawlers()
    assert len(crawlers) == 5


@mock_aws
def test_oedi_glue__start_crawler():
    pass
