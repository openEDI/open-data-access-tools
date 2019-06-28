import boto3
from pprint import pprint

session = boto3.Session(profile_name='nrel-aws-dev', region_name='us-west-2')

client = session.client('glue')

response = client.get_table(DatabaseName='oedi',
    Name='pv_rooftops_aspects'
    )

response2 = client.get_partitions(

    DatabaseName='oedi',
    TableName='pv_rooftops_aspects'
)
response3 = client.get_partitions(

    DatabaseName='oedi',
    TableName='pv_rooftops_aspects',
    NextToken=response2['NextToken']
)
partitionsarray = []
pprint(response)
pprint(response2)
for res in response2['Partitions']:
    print(res)
    try:
        print(res['StorageDescriptor']['Parameters']['averageRecordSize'])
        partitionsarray.append(dict(name=res['Values'][0],
                                    averageRecordSize=res['StorageDescriptor']['Parameters']['averageRecordSize'],
                                    recordCount=res['StorageDescriptor']['Parameters']['recordCount'],
                                    sizeKey=res['StorageDescriptor']['Parameters']['sizeKey']))
    except Exception as e:
        print('EXCEPTION')
        print(str(e))

for res in response3['Partitions']:
    print(res)
    try:
        print(res['StorageDescriptor']['Parameters']['averageRecordSize'])
        partitionsarray.append(dict(name=res['Values'][0],
                                    averageRecordSize=res['StorageDescriptor']['Parameters']['averageRecordSize'],
                                    recordCount=res['StorageDescriptor']['Parameters']['recordCount'],
                                    sizeKey=res['StorageDescriptor']['Parameters']['sizeKey']))
    except Exception as e:
        print('EXCEPTION')
        print(str(e))


pprint(partitionsarray)
pprint(len(partitionsarray))

pprint(response3)

'''
detroit_mi_12

'''