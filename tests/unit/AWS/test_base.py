import mock

from oedi.AWS.base import AWSClientBase


@mock.patch("oedi.AWS.base.boto3.client")
def test_aws_client_base(mock_boto3_client):
    client_base = AWSClientBase("glue")
    assert client_base.service_name == "glue"
    assert client_base.client is not None
    assert mock_boto3_client.called
