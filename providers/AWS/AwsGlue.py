from AwsBase import AwsBase
from botocore.errorfactory import ClientError

class AwsGlue(AwsBase):

    def __init__(self, boto3_sessison, database_name):
        self.boto3_session = boto3_sessison
        self.glue = self.boto3_session.client('glue')
        self.database_name = database_name

    def create_database(self):
        try:
            response = self.glue.create_database(
                DatabaseInput={
                    'Name': self.database_name,
                    'Description': 'Open Energy Data Initiative Glue catalog database'
                }
            )
            print(response)
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(str(e))


    def create_tracking_the_sun_table(self):
            response = self.glue.create_table(
                DatabaseName=self.database_name,
                TableInput={
                    "Name": "oedi_tracking_the_sun3",
                    "StorageDescriptor": {
                        "Columns": [
                            {
                                "Name": "data provider",
                                "Type": "string"
                            },
                            {
                                "Name": "system id (from data provider)",
                                "Type": "string"
                            },
                            {
                                "Name": "system id (tracking the sun)",
                                "Type": "string"
                            },
                            {
                                "Name": "installation date",
                                "Type": "string"
                            },
                            {
                                "Name": "system size",
                                "Type": "double"
                            },
                            {
                                "Name": "total installed price",
                                "Type": "double"
                            },
                            {
                                "Name": "appraised value flag",
                                "Type": "boolean"
                            },
                            {
                                "Name": "sales tax cost",
                                "Type": "double"
                            },
                            {
                                "Name": "rebate or grant",
                                "Type": "double"
                            },
                            {
                                "Name": "performance-based incentive (annual payment)",
                                "Type": "double"
                            },
                            {
                                "Name": "performance-based incentives (duration)",
                                "Type": "bigint"
                            },
                            {
                                "Name": "feed-in tariff (annual payment)",
                                "Type": "bigint"
                            },
                            {
                                "Name": "feed-in tariff (duration)",
                                "Type": "bigint"
                            },
                            {
                                "Name": "customer segment",
                                "Type": "string"
                            },
                            {
                                "Name": "new construction",
                                "Type": "bigint"
                            },
                            {
                                "Name": "tracking",
                                "Type": "bigint"
                            },
                            {
                                "Name": "tracking type",
                                "Type": "string"
                            },
                            {
                                "Name": "ground mounted",
                                "Type": "bigint"
                            },
                            {
                                "Name": "battery system",
                                "Type": "bigint"
                            },
                            {
                                "Name": "zip code",
                                "Type": "bigint"
                            },
                            {
                                "Name": "city",
                                "Type": "string"
                            },
                            {
                                "Name": "county",
                                "Type": "string"
                            },
                            {
                                "Name": "state",
                                "Type": "string"
                            },
                            {
                                "Name": "utility service territory",
                                "Type": "string"
                            },
                            {
                                "Name": "third-party owned",
                                "Type": "bigint"
                            },
                            {
                                "Name": "installer name",
                                "Type": "string"
                            },
                            {
                                "Name": "self-installed",
                                "Type": "bigint"
                            },
                            {
                                "Name": "azimuth #1",
                                "Type": "double"
                            },
                            {
                                "Name": "azimuth #2",
                                "Type": "bigint"
                            },
                            {
                                "Name": "azimuth #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "tilt #1",
                                "Type": "double"
                            },
                            {
                                "Name": "tilt #2",
                                "Type": "bigint"
                            },
                            {
                                "Name": "tilt #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "module manufacturer #1",
                                "Type": "string"
                            },
                            {
                                "Name": "module model #1",
                                "Type": "string"
                            },
                            {
                                "Name": "module manufacturer #2",
                                "Type": "string"
                            },
                            {
                                "Name": "module model #2",
                                "Type": "string"
                            },
                            {
                                "Name": "module manufacturer #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "module model #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "additional module model",
                                "Type": "bigint"
                            },
                            {
                                "Name": "module technology #1",
                                "Type": "string"
                            },
                            {
                                "Name": "module technology #2",
                                "Type": "string"
                            },
                            {
                                "Name": "module technology #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "bipv module #1",
                                "Type": "bigint"
                            },
                            {
                                "Name": "bipv module #2",
                                "Type": "bigint"
                            },
                            {
                                "Name": "bipv module #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "module efficiency #1",
                                "Type": "double"
                            },
                            {
                                "Name": "module efficiency #2",
                                "Type": "double"
                            },
                            {
                                "Name": "module efficiency #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "inverter manufacturer #1",
                                "Type": "string"
                            },
                            {
                                "Name": "inverter model #1",
                                "Type": "string"
                            },
                            {
                                "Name": "inverter quantity #1",
                                "Type": "bigint"
                            },
                            {
                                "Name": "inverter manufacturer #2",
                                "Type": "string"
                            },
                            {
                                "Name": "inverter model #2",
                                "Type": "string"
                            },
                            {
                                "Name": "inverter quantity #2",
                                "Type": "bigint"
                            },
                            {
                                "Name": "inverter manufacturer #3",
                                "Type": "string"
                            },
                            {
                                "Name": "inverter model #3",
                                "Type": "string"
                            },
                            {
                                "Name": "inverter quantity #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "additional inverter model",
                                "Type": "bigint"
                            },
                            {
                                "Name": "microinverter #1",
                                "Type": "bigint"
                            },
                            {
                                "Name": "microinverter #2",
                                "Type": "bigint"
                            },
                            {
                                "Name": "microinverter #3",
                                "Type": "bigint"
                            },
                            {
                                "Name": "dc optimizer",
                                "Type": "bigint"
                            }
                        ],
                        "Location": "s3://oedi-dev-tracking-the-sun/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "NumberOfBuckets": -1,
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                            "Parameters": {
                                "field.delim": ","
                            }
                        },


                    },
                    "Parameters": {
                        "areColumnsQuoted": "false",
                        "averageRecordSize": "677",
                        "classification": "csv",
                        "columnsOrdered": "true",
                        "compressionType": "zip",
                        "delimiter": ",",
                        "exclusions": "[\"s3://*/*.pdf\"]",
                        "objectCount": "2",
                        "recordCount": "53300",
                        "sizeKey": "51232868",
                        "skip.header.line.count": "1",
                        "typeOfData": "file"
                    }

                }
            )

