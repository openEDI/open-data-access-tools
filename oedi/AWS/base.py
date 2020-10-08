import boto3


class AWSClientBase(object):

    def __init__(self, service_name, **kwargs):
        self._service_name = service_name
        self._kwargs = kwargs
        self._client = None

    @property
    def service_name(self):
        return self._service_name

    @property
    def client(self):
        if self._client is None:
            self._client = self.connect()
        return self._client

    def connect(self):
        """Establish client connection object"""
        return boto3.client(self.service_name, **self._kwargs)
