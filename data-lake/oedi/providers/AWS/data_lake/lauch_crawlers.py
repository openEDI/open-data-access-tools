import boto3

client = boto3.client('glue', 'us-west-2')


client.start_crawler(
    Name='oedi-nrel-garage-array'
)

client.start_crawler(
    Name='oedi-nrel-rsf-array'
)

client.start_crawler(
    Name='oedi-nrel-stf-array'
)

client.start_crawler(
    Name='oedi-nrel-windsite-array'
)

client.start_crawler(
    Name='oedi-tracking-the-sun'
)

client.start_crawler(
    Name='oedi-pv-rooftops'
)



