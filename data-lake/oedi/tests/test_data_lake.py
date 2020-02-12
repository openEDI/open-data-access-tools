import boto3
from botocore.errorfactory import ClientError
import time
from pprint import pprint


client = boto3.client('athena')
gclient = boto3.client('glue')

class TestClass(object):

    def test_tracking_the_sun_query(self):
        response = client.start_query_execution(
            QueryString = 'SELECT "system_size" FROM "oedidb"."oedi_dev_tracking_the_sun" where "sales_tax_cost" = 725.386027',
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


        x = 0
        theAnswer = ''
        while x < 10:
            try:
                response3 = client.get_query_execution(
                    QueryExecutionId=response['QueryExecutionId']
                )
                #pprint(response3)
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
                    #print('sleeping')
                elif 'FAILED' in str(e):
                    raise
                else:
                    time.sleep(5)

        assert theAnswer == '7.811037539'

    def test_pv_rooftops_buildings_query(self):
        response = client.start_query_execution(

            QueryString = "SELECT region_id FROM oedidb.pv_rooftops_buildings where gid = 3120 and city_year = 'pierre_sd_08'",
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
                #pprint(response3)
                response2 = client.get_query_results(
                    QueryExecutionId=response['QueryExecutionId']
                )
                #pprint(response2)
                for row in response2['ResultSet']['Rows']:
                    #print(row)
                    if row['Data'][0]['VarCharValue'] != 'region_id':
                        theAnswer =  row['Data'][0]['VarCharValue']
                break
            except Exception as e:
                #print(e)
                if 'QUEUED' in str(e):
                    time.sleep(5)
                    #print('sleeping')
                elif 'FAILED' in str(e):
                    raise
                else:
                    time.sleep(5)
        #print(theAnswer)

        assert theAnswer == '118'

    def test_pv_rooftops_buildings_partitions(self):
        count = 0
        response = gclient.get_partitions(

            DatabaseName='oedidb',
            TableName='pv_rooftops_buildings'

        )

        #pprint(response)
        next_token = response['NextToken']
        #pprint(next_token)

        for part in response['Partitions']:
            count = count + 1

        #print("NEXT")
        response2 = gclient.get_partitions(

            DatabaseName='oedidb',
            TableName='pv_rooftops_buildings',
            NextToken=next_token
        )
        #pprint(response2)
        for part in response2['Partitions']:
            count = count + 1

        #print(count)
        # TODO: Insure this is actually supposed to be the correct count... :)
        assert count == 168

    def test_stf_array_query(self):
        response = client.start_query_execution(

            QueryString = "SELECT count(*) FROM oedidb.oedi_stf_array where year = '2009' and month = '10'",
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
                #pprint(response3)
                response2 = client.get_query_results(
                    QueryExecutionId=response['QueryExecutionId']
                )
                #pprint(response2)
                for row in response2['ResultSet']['Rows']:
                    #print(row)
                    if row['Data'][0]['VarCharValue'] != 'region_id':
                        theAnswer =  row['Data'][0]['VarCharValue']
                break
            except Exception as e:
                #print(e)
                if 'QUEUED' in str(e):
                    time.sleep(5)
                    #print('sleeping')
                elif 'FAILED' in str(e):
                    raise
                else:
                    time.sleep(5)

        #print(theAnswer)

        assert theAnswer == '14662'



    def test_windsite_array_query(self):
        response = client.start_query_execution(

            QueryString = "SELECT count(*) FROM oedidb.oedi_windsite_array where year = '2017' and month = '10'",
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
                #pprint(response3)
                response2 = client.get_query_results(
                    QueryExecutionId=response['QueryExecutionId']
                )
                #pprint(response2)
                for row in response2['ResultSet']['Rows']:
                    #print(row)
                    if row['Data'][0]['VarCharValue'] != 'region_id':
                        theAnswer =  row['Data'][0]['VarCharValue']
                break
            except Exception as e:
                #print(e)
                if 'QUEUED' in str(e):
                    time.sleep(5)
                    #print('sleeping')
                elif 'FAILED' in str(e):
                    raise
                else:
                    time.sleep(5)

        #print(theAnswer)

        assert theAnswer == '25290'



    def test_rsf_array_query(self):
        response = client.start_query_execution(

            QueryString = "SELECT count(*) FROM oedidb.oedi_rsf_array where year = '2017' and month = '10'",
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
                #pprint(response3)
                response2 = client.get_query_results(
                    QueryExecutionId=response['QueryExecutionId']
                )
                #pprint(response2)
                for row in response2['ResultSet']['Rows']:
                    #print(row)
                    if row['Data'][0]['VarCharValue'] != 'region_id':
                        theAnswer =  row['Data'][0]['VarCharValue']
                break
            except Exception as e:
                #print(e)
                if 'QUEUED' in str(e):
                    time.sleep(5)
                    #print('sleeping')
                elif 'FAILED' in str(e):
                    raise
                else:
                    time.sleep(5)

        #print(theAnswer)

        assert theAnswer == '3747901'



    def test_garage_array_query(self):
        response = client.start_query_execution(

            QueryString = "SELECT count(*) FROM oedidb.oedi_garage_array where year = '2017' and month = '10'",
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
                #pprint(response3)
                response2 = client.get_query_results(
                    QueryExecutionId=response['QueryExecutionId']
                )
                #pprint(response2)
                for row in response2['ResultSet']['Rows']:
                    #print(row)
                    if row['Data'][0]['VarCharValue'] != 'region_id':
                        theAnswer =  row['Data'][0]['VarCharValue']
                break
            except Exception as e:
                #print(e)
                if 'QUEUED' in str(e):
                    time.sleep(5)
                    #print('sleeping')
                elif 'FAILED' in str(e):
                    raise
                else:
                    time.sleep(5)

        #print(theAnswer)

        assert theAnswer == '3011847'

#a = TestClass()
#a.test_stf_array_query()