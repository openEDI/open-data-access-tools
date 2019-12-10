import boto3
from pprint import pprint
session = boto3.Session(profile_name='nrel-aws-dev')
client = session.client("s3", region_name='us-west-2')

response = client.list_buckets()

pprint(response)

response2 = client.list_objects(Bucket='oedi-pv-rooftops')

pprint(response2)