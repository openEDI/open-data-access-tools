import boto3
from botocore.errorfactory import ClientError
import time
from pprint import pprint


client = boto3.client('athena')

class TestClass(object):

    def test_tracking_the_sun(self):
        response = client.start_query_execution(
            #QueryString='SELECT "system size" * FROM "oedidb"."oedi_tracking_the_sun" where "sales tax cost" = 725.386027;',
            QueryString = 'SELECT "system size" FROM "oedidb"."oedi_tracking_the_sun" where "sales tax cost" = 725.386027',
            QueryExecutionContext={
                'Database': 'oedidb'
            },
            ResultConfiguration={
                'OutputLocation': 's3://aws-athena-query-results-501953089731-us-west-2/',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }
        )
        #print(response)

        x = 0
        theAnswer = ''
        while x < 10:
            try:
                response3 = client.get_query_execution(
                    QueryExecutionId=response['QueryExecutionId']
                )
                pprint(response3)
                response2 = client.get_query_results(
                    QueryExecutionId=response['QueryExecutionId']
                )
                #pprint(response2)
                for row in response2['ResultSet']['Rows']:
                    #print(row)
                    if row['Data'][0]['VarCharValue'] != 'system size':
                        theAnswer =  row['Data'][0]['VarCharValue']
                break
            except Exception as e:
                #print(e)
                if 'QUEUED' in str(e):
                    time.sleep(5)
                    print('sleeping')
                elif 'FAILED' in str(e):
                    raise
                else:
                    time.sleep(5)

        assert theAnswer == '7.811037539'

a = TestClass()
a.test_tracking_the_sun()