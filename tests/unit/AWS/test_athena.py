import mock

from moto.athena import mock_aws

from oedi.AWS.athena import OEDIAthena


@mock_aws
@mock.patch("oedi.AWS.athena.Connection")
def test_oedi_athena__properties(mock_connection):
    staging_location = "s3://my-testing-bucket/"
    region_name = "us-west-2"
    athena = OEDIAthena(staging_location, region_name)

    # Assertion
    assert athena.staging_location == staging_location
    assert athena.region_name == region_name
    assert athena.conn is not None

    assert mock_connection.called


@mock_aws
@mock.patch("oedi.AWS.athena.Connection")
@mock.patch("oedi.AWS.athena.OEDIAthena._pandas_cursor_execute")
@mock.patch("oedi.AWS.athena.OEDIAthena._load_wkt")
def test_oedi_athena__run_query(mock_execute, mock_load_wkt, mock_connection):
    staging_location = "s3://my-testing-bucket/"
    region_name = "us-west-2"
    athena = OEDIAthena(staging_location, region_name)

    sql = "SELECT * FROM table LIMIT 10;"
    geometry = "geom_4326"
    athena.run_query(sql, geometry)

    assert mock_execute.called
    assert mock_load_wkt.called
