from .AwsBase import AwsBase
from botocore.errorfactory import ClientError
import logging
from pprint import pprint

logger = logging.getLogger("OEDI")
print(__name__)

class AwsGlue(AwsBase):

    def __init__(self, boto3_sessison, database_name):
        super().__init__()
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
            #print(response)

        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.info("Database already exists and will be modified")

    def create_tracking_the_sun_table(self):

        try:
            logger.info(f"Recreating table: {self.tracking_the_sun_table_name}")
            response = self.glue.delete_table(
                DatabaseName=self.database_name,
                Name=self.tracking_the_sun_table_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.debug(r"Skipping table deletion as it does not exist: {self.tracking_the_sun_table_name}")

        response = self.glue.create_table(
            DatabaseName=self.database_name,
            TableInput={
                "Name": self.tracking_the_sun_table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "data_provider",
                            "Type": "string"
                        },
                        {
                            "Name": "system_id_from_data_provider",
                            "Type": "string"
                        },
                        {
                            "Name": "system_id_tracking_the_sun",
                            "Type": "string"
                        },
                        {
                            "Name": "installation_date",
                            "Type": "string"
                        },
                        {
                            "Name": "system_size",
                            "Type": "double"
                        },
                        {
                            "Name": "total_installed_price",
                            "Type": "double"
                        },
                        {
                            "Name": "appraised_value_flag",
                            "Type": "boolean"
                        },
                        {
                            "Name": "sales_tax_cost",
                            "Type": "double"
                        },
                        {
                            "Name": "rebate_or_grant",
                            "Type": "double"
                        },
                        {
                            "Name": "performance_based_incentive_annual_payment",
                            "Type": "double"
                        },
                        {
                            "Name": "performance_based_incentives_duration",
                            "Type": "bigint"
                        },
                        {
                            "Name": "feed_in_tariff_annual_payment",
                            "Type": "bigint"
                        },
                        {
                            "Name": "feed_in_tariff_duration",
                            "Type": "bigint"
                        },
                        {
                            "Name": "customer_segment",
                            "Type": "string"
                        },
                        {
                            "Name": "new_construction",
                            "Type": "bigint"
                        },
                        {
                            "Name": "tracking",
                            "Type": "bigint"
                        },
                        {
                            "Name": "tracking_type",
                            "Type": "string"
                        },
                        {
                            "Name": "ground_mounted",
                            "Type": "bigint"
                        },
                        {
                            "Name": "battery_system",
                            "Type": "bigint"
                        },
                        {
                            "Name": "zip_code",
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
                            "Name": "utility_service_territory",
                            "Type": "string"
                        },
                        {
                            "Name": "third_party_owned",
                            "Type": "bigint"
                        },
                        {
                            "Name": "installer_name",
                            "Type": "string"
                        },
                        {
                            "Name": "self_installed",
                            "Type": "bigint"
                        },
                        {
                            "Name": "azimuth_1",
                            "Type": "double"
                        },
                        {
                            "Name": "azimuth_2",
                            "Type": "bigint"
                        },
                        {
                            "Name": "azimuth_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "tilt_1",
                            "Type": "double"
                        },
                        {
                            "Name": "tilt_2",
                            "Type": "bigint"
                        },
                        {
                            "Name": "tilt_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "module_manufacturer_1",
                            "Type": "string"
                        },
                        {
                            "Name": "module_model_1",
                            "Type": "string"
                        },
                        {
                            "Name": "module_manufacturer_2",
                            "Type": "string"
                        },
                        {
                            "Name": "module_model_2",
                            "Type": "string"
                        },
                        {
                            "Name": "module_manufacturer_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "module_model_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "additional_module_model",
                            "Type": "bigint"
                        },
                        {
                            "Name": "module_technology_1",
                            "Type": "string"
                        },
                        {
                            "Name": "module_technology_2",
                            "Type": "string"
                        },
                        {
                            "Name": "module_technology_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "bipv_module_1",
                            "Type": "bigint"
                        },
                        {
                            "Name": "bipv_module_2",
                            "Type": "bigint"
                        },
                        {
                            "Name": "bipv_module_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "module_efficiency_1",
                            "Type": "double"
                        },
                        {
                            "Name": "module_efficiency_2",
                            "Type": "double"
                        },
                        {
                            "Name": "module_efficiency_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "inverter_manufacturer_1",
                            "Type": "string"
                        },
                        {
                            "Name": "inverter_model_1",
                            "Type": "string"
                        },
                        {
                            "Name": "inverter_quantity_1",
                            "Type": "bigint"
                        },
                        {
                            "Name": "inverter_manufacturer_2",
                            "Type": "string"
                        },
                        {
                            "Name": "inverter_model_2",
                            "Type": "string"
                        },
                        {
                            "Name": "inverter_quantity_2",
                            "Type": "bigint"
                        },
                        {
                            "Name": "inverter_manufacturer_3",
                            "Type": "string"
                        },
                        {
                            "Name": "inverter_model_3",
                            "Type": "string"
                        },
                        {
                            "Name": "inverter_quantity_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "additional_inverter_model",
                            "Type": "bigint"
                        },
                        {
                            "Name": "microinverter_1",
                            "Type": "bigint"
                        },
                        {
                            "Name": "microinverter_2",
                            "Type": "bigint"
                        },
                        {
                            "Name": "microinverter_3",
                            "Type": "bigint"
                        },
                        {
                            "Name": "dc_optimizer",
                            "Type": "bigint"
                        }
                    ],
                    "Location": "s3://oedi-dev-tracking-the-sun/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "NumberOfBuckets": -1,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
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

    def get_pv_rooftop_bldg_partition_template(self):
        return {
            'StorageDescriptor': {'BucketColumns': [],
                                  'Columns': [{'Name': 'gid',
                                               'Type': 'bigint'},
                                              {'Name': 'bldg_fid',
                                               'Type': 'bigint'},
                                              {'Name': 'the_geom_96703',
                                               'Type': 'string'},
                                              {'Name': 'the_geom_4326',
                                               'Type': 'string'},
                                              {'Name': 'city',
                                               'Type': 'string'},
                                              {'Name': 'state',
                                               'Type': 'string'},
                                              {'Name': 'year',
                                               'Type': 'bigint'},
                                              {'Name': 'region_id',
                                               'Type': 'bigint'},
                                              {'Name': '__index_level_0__',
                                               'Type': 'bigint'}],
                                  'Compressed': False,
                                  'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                                  'Location': '',
                                  'NumberOfBuckets': -1,
                                  'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                                  'Parameters': {'averageRecordSize': '',
                                                 'classification': 'parquet',
                                                 'compressionType': 'none',
                                                 'objectCount': '1',
                                                 'recordCount': '',
                                                 'sizeKey': '',
                                                 'typeOfData': 'file'},
                                  'SerdeInfo': {'Parameters': {'serialization.format': '1'},
                                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'},
                                  'SortColumns': [],
                                  'StoredAsSubDirectories': False},
            'Values': ['']}



    def create_pv_rooftop_buildings_table(self):

        partitionsArray = [{'averageRecordSize': '253',
                              'name': 'chicago_il_08',
                              'recordCount': '903404',
                              'sizeKey': '229002201'},
                             {'averageRecordSize': '304',
                              'name': 'springfield_ma_13',
                              'recordCount': '114405',
                              'sizeKey': '34879403'},
                             {'averageRecordSize': '325',
                              'name': 'elpaso_tx_07',
                              'recordCount': '111316',
                              'sizeKey': '36252660'},
                             {'averageRecordSize': '286',
                              'name': 'lubbock_tx_08',
                              'recordCount': '66388',
                              'sizeKey': '19045494'},
                             {'averageRecordSize': '262',
                              'name': 'fortwayne_in_08',
                              'recordCount': '71043',
                              'sizeKey': '18681955'},
                             {'averageRecordSize': '279',
                              'name': 'providence_ri_12',
                              'recordCount': '213344',
                              'sizeKey': '59662679'},
                             {'averageRecordSize': '305',
                              'name': 'augusta_ga_10',
                              'recordCount': '141891',
                              'sizeKey': '43367684'},
                             {'averageRecordSize': '328',
                              'name': 'orlando_fl_09',
                              'recordCount': '131260',
                              'sizeKey': '43069457'},
                             {'averageRecordSize': '282',
                              'name': 'columbus_ga_09',
                              'recordCount': '62932',
                              'sizeKey': '17752691'},
                             {'averageRecordSize': '278',
                              'name': 'neworleans_la_08',
                              'recordCount': '199835',
                              'sizeKey': '55638038'},
                             {'averageRecordSize': '278',
                              'name': 'omaha_ne_07',
                              'recordCount': '140418',
                              'sizeKey': '39112092'},
                             {'averageRecordSize': '294',
                              'name': 'poughkeepsie_ny_12',
                              'recordCount': '115295',
                              'sizeKey': '33907860'},
                             {'averageRecordSize': '309',
                              'name': 'charlotte_nc_12',
                              'recordCount': '220728',
                              'sizeKey': '68310527'},
                             {'averageRecordSize': '277',
                              'name': 'kansascity_mo_10',
                              'recordCount': '217167',
                              'sizeKey': '60181733'},
                             {'averageRecordSize': '407',
                              'name': 'washington_dc_12',
                              'recordCount': '40370',
                              'sizeKey': '16472838'},
                             {'averageRecordSize': '344',
                              'name': 'atlanta_ga_08',
                              'recordCount': '41789',
                              'sizeKey': '14410342'},
                             {'averageRecordSize': '284',
                              'name': 'bridgeport_ct_06',
                              'recordCount': '104902',
                              'sizeKey': '29817578'},
                             {'averageRecordSize': '289',
                              'name': 'columbia_sc_09',
                              'recordCount': '61853',
                              'sizeKey': '17937172'},
                             {'averageRecordSize': '302',
                              'name': 'sanantonio_tx_13',
                              'recordCount': '513597',
                              'sizeKey': '155568411'},
                             {'averageRecordSize': '294',
                              'name': 'tucson_az_07',
                              'recordCount': '152548',
                              'sizeKey': '44956993'},
                             {'averageRecordSize': '293',
                              'name': 'indianapolis_in_12',
                              'recordCount': '387999',
                              'sizeKey': '113985977'},
                             {'averageRecordSize': '265',
                              'name': 'syracuse_ny_08',
                              'recordCount': '81519',
                              'sizeKey': '21664878'},
                             {'averageRecordSize': '280',
                              'name': 'coloradosprings_co_06',
                              'recordCount': '108381',
                              'sizeKey': '30373902'},
                             {'averageRecordSize': '332',
                              'name': 'lancaster_pa_10',
                              'recordCount': '73403',
                              'sizeKey': '24408868'},
                             {'averageRecordSize': '281',
                              'name': 'lexington_ky_12',
                              'recordCount': '90510',
                              'sizeKey': '25519182'},
                             {'averageRecordSize': '303',
                              'name': 'shreveport_la_08',
                              'recordCount': '122955',
                              'sizeKey': '37353151'},
                             {'averageRecordSize': '307',
                              'name': 'newyork_ny_13',
                              'recordCount': '1070937',
                              'sizeKey': '329711038'},
                             {'averageRecordSize': '303',
                              'name': 'albany_ny_13',
                              'recordCount': '164978',
                              'sizeKey': '50139844'},
                             {'averageRecordSize': '303',
                              'name': 'bridgeport_ct_13',
                              'recordCount': '110913',
                              'sizeKey': '33617054'},
                             {'averageRecordSize': '253',
                              'name': 'buffalo_ny_08',
                              'recordCount': '298360',
                              'sizeKey': '75600281'},
                             {'averageRecordSize': '269',
                              'name': 'seattle_wa_11',
                              'recordCount': '138104',
                              'sizeKey': '37216766'},
                             {'averageRecordSize': '265',
                              'name': 'pittsburgh_pa_12',
                              'recordCount': '398202',
                              'sizeKey': '105664218'},
                             {'averageRecordSize': '282',
                              'name': 'charleston_wv_09',
                              'recordCount': '54046',
                              'sizeKey': '15275544'},
                             {'averageRecordSize': '300',
                              'name': 'huntsville_al_09',
                              'recordCount': '104122',
                              'sizeKey': '31254812'},
                             {'averageRecordSize': '322',
                              'name': 'pensacola_fl_09',
                              'recordCount': '97179',
                              'sizeKey': '31299078'},
                             {'averageRecordSize': '284',
                              'name': 'stlouis_mo_13',
                              'recordCount': '517103',
                              'sizeKey': '147229372'},
                             {'averageRecordSize': '293',
                              'name': 'tampa_fl_08',
                              'recordCount': '101793',
                              'sizeKey': '29885564'},
                             {'averageRecordSize': '288',
                              'name': 'topeka_ks_08',
                              'recordCount': '70601',
                              'sizeKey': '20341092'},
                             {'averageRecordSize': '276',
                              'name': 'providence_ri_04',
                              'recordCount': '164929',
                              'sizeKey': '45659502'},
                             {'averageRecordSize': '294',
                              'name': 'sacramento_ca_12',
                              'recordCount': '239854',
                              'sizeKey': '70729549'},
                             {'averageRecordSize': '268',
                              'name': 'youngstown_oh_08',
                              'recordCount': '197154',
                              'sizeKey': '52898772'},
                             {'averageRecordSize': '299',
                              'name': 'carsoncity_nv_09',
                              'recordCount': '27006',
                              'sizeKey': '8089251'},
                             {'averageRecordSize': '300',
                              'name': 'coloradosprings_co_13',
                              'recordCount': '177577',
                              'sizeKey': '53452976'},
                             {'averageRecordSize': '288',
                              'name': 'harrisburg_pa_09',
                              'recordCount': '171811',
                              'sizeKey': '49496874'},
                             {'averageRecordSize': '304',
                              'name': 'jacksonville_fl_10',
                              'recordCount': '156647',
                              'sizeKey': '47643046'},
                             {'averageRecordSize': '292',
                              'name': 'pittsburgh_pa_04',
                              'recordCount': '183672',
                              'sizeKey': '53697486'},
                             {'averageRecordSize': '255',
                              'name': 'stlouis_mo_08',
                              'recordCount': '233371',
                              'sizeKey': '59647142'},
                             {'averageRecordSize': '299',
                              'name': 'tallahassee_fl_09',
                              'recordCount': '61968',
                              'sizeKey': '18542164'},
                             {'averageRecordSize': '280',
                              'name': 'mcallen_tx_08',
                              'recordCount': '97965',
                              'sizeKey': '27513547'},
                             {'averageRecordSize': '317',
                              'name': 'sanbernardinoriverside_ca_12',
                              'recordCount': '261248',
                              'sizeKey': '83051331'},
                             {'averageRecordSize': '335',
                              'name': 'sandiego_ca_13',
                              'recordCount': '121639',
                              'sizeKey': '40776290'},
                             {'averageRecordSize': '288',
                              'name': 'indianapolis_in_06',
                              'recordCount': '145320',
                              'sizeKey': '41887068'},
                             {'averageRecordSize': '303',
                              'name': 'minneapolis_mn_12',
                              'recordCount': '752775',
                              'sizeKey': '228392184'},
                             {'averageRecordSize': '288',
                              'name': 'lasvegas_nv_09',
                              'recordCount': '475543',
                              'sizeKey': '136969563'},
                             {'averageRecordSize': '301',
                              'name': 'charleston_sc_10',
                              'recordCount': '39791',
                              'sizeKey': '12010183'},
                             {'averageRecordSize': '285',
                              'name': 'richmond_va_13',
                              'recordCount': '64437',
                              'sizeKey': '18374395'},
                             {'averageRecordSize': '298',
                              'name': 'rochester_ny_14',
                              'recordCount': '119101',
                              'sizeKey': '35512853'},
                             {'averageRecordSize': '271',
                              'name': 'desmoines_ia_10',
                              'recordCount': '149385',
                              'sizeKey': '40582578'},
                             {'averageRecordSize': '285',
                              'name': 'lansing_mi_07',
                              'recordCount': '121708',
                              'sizeKey': '34803858'},
                             {'averageRecordSize': '402',
                              'name': 'batonrouge_la_06',
                              'recordCount': '102329',
                              'sizeKey': '41174433'},
                             {'averageRecordSize': '311',
                              'name': 'charlotte_nc_06',
                              'recordCount': '95913',
                              'sizeKey': '29857014'},
                             {'averageRecordSize': '271',
                              'name': 'chicago_il_12',
                              'recordCount': '1071844',
                              'sizeKey': '291121258'},
                             {'averageRecordSize': '282',
                              'name': 'saltlakecity_ut_12',
                              'recordCount': '214042',
                              'sizeKey': '60407802'},
                             {'averageRecordSize': '314',
                              'name': 'jackson_ms_07',
                              'recordCount': '91891',
                              'sizeKey': '28904321'},
                             {'averageRecordSize': '271',
                              'name': 'newhaven_ct_07',
                              'recordCount': '177767',
                              'sizeKey': '48308089'},
                             {'averageRecordSize': '284',
                              'name': 'amarillo_tx_08',
                              'recordCount': '70472',
                              'sizeKey': '20051787'},
                             {'averageRecordSize': '273',
                              'name': 'cincinnati_oh_10',
                              'recordCount': '204496',
                              'sizeKey': '55905781'},
                             {'averageRecordSize': '305',
                              'name': 'austin_tx_06',
                              'recordCount': '108446',
                              'sizeKey': '33116610'},
                             {'averageRecordSize': '282',
                              'name': 'minneapolis_mn_07',
                              'recordCount': '545711',
                              'sizeKey': '153904864'},
                             {'averageRecordSize': '310',
                              'name': 'sanfrancisco_ca_13',
                              'recordCount': '921012',
                              'sizeKey': '285844206'},
                             {'averageRecordSize': '278',
                              'name': 'helena_mt_07',
                              'recordCount': '26528',
                              'sizeKey': '7387340'},
                             {'averageRecordSize': '263',
                              'name': 'cleveland_oh_12',
                              'recordCount': '450403',
                              'sizeKey': '118898307'},
                             {'averageRecordSize': '275',
                              'name': 'trenton_nj_08',
                              'recordCount': '54743',
                              'sizeKey': '15103325'},
                             {'averageRecordSize': '315',
                              'name': 'austin_tx_12',
                              'recordCount': '86212',
                              'sizeKey': '27172664'},
                             {'averageRecordSize': '277',
                              'name': 'montpelier_vt_09',
                              'recordCount': '12043',
                              'sizeKey': '3346948'},
                             {'averageRecordSize': '382',
                              'name': 'batonrouge_la_12',
                              'recordCount': '104970',
                              'sizeKey': '40138735'},
                             {'averageRecordSize': '319',
                              'name': 'sarasota_fl_09',
                              'recordCount': '142167',
                              'sizeKey': '45460899'},
                             {'averageRecordSize': '280',
                              'name': 'allentown_pa_06',
                              'recordCount': '121875',
                              'sizeKey': '34243372'},
                             {'averageRecordSize': '289',
                              'name': 'sanantonio_tx_08',
                              'recordCount': '440676',
                              'sizeKey': '127738116'},
                             {'averageRecordSize': '487',
                              'name': 'boulder_co_14',
                              'recordCount': '41041',
                              'sizeKey': '20020641'},
                             {'averageRecordSize': '317',
                              'name': 'stockton_ca_10',
                              'recordCount': '110711',
                              'sizeKey': '35195282'},
                             {'averageRecordSize': '281',
                              'name': 'winstonsalem_nc_09',
                              'recordCount': '107924',
                              'sizeKey': '30389558'},
                             {'averageRecordSize': '347',
                              'name': 'corpuschristi_tx_12',
                              'recordCount': '33697',
                              'sizeKey': '11721412'},
                             {'averageRecordSize': '306',
                              'name': 'greensboro_nc_09',
                              'recordCount': '110206',
                              'sizeKey': '33765607'},
                             {'averageRecordSize': '269',
                              'name': 'newark_nj_07',
                              'recordCount': '170068',
                              'sizeKey': '45838685'},
                             {'averageRecordSize': '286',
                              'name': 'toledo_oh_12',
                              'recordCount': '168134',
                              'sizeKey': '48226755'},
                             {'averageRecordSize': '277',
                              'name': 'worcester_ma_09',
                              'recordCount': '72703',
                              'sizeKey': '20188431'},
                             {'averageRecordSize': '326',
                              'name': 'palmbay_fl_10',
                              'recordCount': '225761',
                              'sizeKey': '73801651'},
                             {'averageRecordSize': '299',
                              'name': 'springfield_il_09',
                              'recordCount': '73573',
                              'sizeKey': '22018437'},
                             {'averageRecordSize': '301',
                              'name': 'ftbelvoir_dc_12',
                              'recordCount': '28599',
                              'sizeKey': '8641512'},
                             {'averageRecordSize': '348',
                              'name': 'miami_fl_09',
                              'recordCount': '585213',
                              'sizeKey': '204170564'},
                             {'averageRecordSize': '292',
                              'name': 'toledo_oh_06',
                              'recordCount': '152813',
                              'sizeKey': '44628698'},
                             {'averageRecordSize': '281',
                              'name': 'sandiego_ca_08',
                              'recordCount': '122549',
                              'sizeKey': '34558569'},
                             {'averageRecordSize': '277',
                              'name': 'springfield_ma_07',
                              'recordCount': '114309',
                              'sizeKey': '31694214'},
                             {'averageRecordSize': '329',
                              'name': 'santafe_nm_09',
                              'recordCount': '40828',
                              'sizeKey': '13442635'},
                             {'averageRecordSize': '323',
                              'name': 'bakersfield_ca_10',
                              'recordCount': '161383',
                              'sizeKey': '52274485'},
                             {'averageRecordSize': '285',
                              'name': 'helena_mt_13',
                              'recordCount': '27042',
                              'sizeKey': '7727396'},
                             {'averageRecordSize': '316',
                              'name': 'oxnard_ca_10',
                              'recordCount': '89242',
                              'sizeKey': '28284011'},
                             {'averageRecordSize': '285',
                              'name': 'kansascity_mo_12',
                              'recordCount': '302506',
                              'sizeKey': '86397328'},
                             {'averageRecordSize': '332',
                              'name': 'missionviejo_ca_13',
                              'recordCount': '86122',
                              'sizeKey': '28597480'},
                             {'averageRecordSize': '303',
                              'name': 'newhaven_ct_13',
                              'recordCount': '224298',
                              'sizeKey': '67969105'},
                             {'averageRecordSize': '294',
                              'name': 'philadelphia_pa_07',
                              'recordCount': '648953',
                              'sizeKey': '191291895'},
                             {'averageRecordSize': '289',
                              'name': 'raleighdurham_nc_10',
                              'recordCount': '181532',
                              'sizeKey': '52495335'},
                             {'averageRecordSize': '283',
                              'name': 'louisville_ky_06',
                              'recordCount': '191009',
                              'sizeKey': '54065517'},
                             {'averageRecordSize': '275',
                              'name': 'spokane_wa_08',
                              'recordCount': '157903',
                              'sizeKey': '43574902'},
                             {'averageRecordSize': '297',
                              'name': 'andrewsafb_dc_12',
                              'recordCount': '5822',
                              'sizeKey': '1737472'},
                             {'averageRecordSize': '277',
                              'name': 'augusta_me_08',
                              'recordCount': '17431',
                              'sizeKey': '4841000'},
                             {'averageRecordSize': '284',
                              'name': 'flint_mi_09',
                              'recordCount': '161558',
                              'sizeKey': '46015169'},
                             {'averageRecordSize': '303',
                              'name': 'frankfort_ky_12',
                              'recordCount': '19369',
                              'sizeKey': '5882154'},
                             {'averageRecordSize': '277',
                              'name': 'milwaukee_wi_13',
                              'recordCount': '399572',
                              'sizeKey': '110964968'},
                             {'averageRecordSize': '291',
                              'name': 'bismarck_nd_08',
                              'recordCount': '35108',
                              'sizeKey': '10255719'},
                             {'averageRecordSize': '280',
                              'name': 'cheyenne_wy_08',
                              'recordCount': '34679',
                              'sizeKey': '9737399'},
                             {'averageRecordSize': '269',
                              'name': 'laguardiajfk_ny_07',
                              'recordCount': '310219',
                              'sizeKey': '83558867'},
                             {'averageRecordSize': '371',
                              'name': 'tampa_fl_13',
                              'recordCount': '515492',
                              'sizeKey': '191715675'},
                             {'averageRecordSize': '304',
                              'name': 'boise_id_07',
                              'recordCount': '117058',
                              'sizeKey': '35614823'},
                             {'averageRecordSize': '271',
                              'name': 'columbus_oh_06',
                              'recordCount': '203820',
                              'sizeKey': '55381428'},
                             {'averageRecordSize': '281',
                              'name': 'hartford_ct_06',
                              'recordCount': '117270',
                              'sizeKey': '33048748'},
                             {'averageRecordSize': '310',
                              'name': 'littlerock_ar_08',
                              'recordCount': '102399',
                              'sizeKey': '31774042'},
                             {'averageRecordSize': '605',
                              'name': 'manhattan_ny_07',
                              'recordCount': '36632',
                              'sizeKey': '22175268'},
                             {'averageRecordSize': '288',
                              'name': 'losangeles_ca_07',
                              'recordCount': '2602094',
                              'sizeKey': '749861961'},
                             {'averageRecordSize': '277',
                              'name': 'pierre_sd_08',
                              'recordCount': '7503',
                              'sizeKey': '2089444'},
                             {'averageRecordSize': '260',
                              'name': 'detroit_mi_12',
                              'recordCount': '647001',
                              'sizeKey': '168401862'},
                             {'averageRecordSize': '272',
                              'name': 'rochester_ny_08',
                              'recordCount': '130811',
                              'sizeKey': '35605414'},
                             {'averageRecordSize': '307',
                              'name': 'fresno_ca_06',
                              'recordCount': '185015',
                              'sizeKey': '56810869'},
                             {'averageRecordSize': '299',
                              'name': 'olympia_wa_10',
                              'recordCount': '72859',
                              'sizeKey': '21834125'},
                             {'averageRecordSize': '272',
                              'name': 'louisville_ky_12',
                              'recordCount': '211012',
                              'sizeKey': '57562436'},
                             {'averageRecordSize': '272',
                              'name': 'arnold_mo_06',
                              'recordCount': '23510',
                              'sizeKey': '6413134'},
                             {'averageRecordSize': '334',
                              'name': 'baltimore_md_13',
                              'recordCount': '190005',
                              'sizeKey': '63485009'},
                             {'averageRecordSize': '349',
                              'name': 'neworleans_la_12',
                              'recordCount': '274754',
                              'sizeKey': '95989288'},
                             {'averageRecordSize': '340',
                              'name': 'boise_id_13',
                              'recordCount': '128429',
                              'sizeKey': '43769495'},
                             {'averageRecordSize': '306',
                              'name': 'omaha_ne_13',
                              'recordCount': '265846',
                              'sizeKey': '81578437'},
                             {'averageRecordSize': '328',
                              'name': 'grandrapids_mi_13',
                              'recordCount': '174433',
                              'sizeKey': '57337256'},
                             {'averageRecordSize': '285',
                              'name': 'washington_dc_09',
                              'recordCount': '1186138',
                              'sizeKey': '338285071'},
                             {'averageRecordSize': '290',
                              'name': 'dayton_oh_12',
                              'recordCount': '193235',
                              'sizeKey': '56045235'},
                             {'averageRecordSize': '307',
                              'name': 'lansing_mi_13',
                              'recordCount': '124492',
                              'sizeKey': '38288103'},
                             {'averageRecordSize': '308',
                              'name': 'modesto_ca_10',
                              'recordCount': '79866',
                              'sizeKey': '24681152'},
                             {'averageRecordSize': '314',
                              'name': 'reno_nv_07',
                              'recordCount': '116640',
                              'sizeKey': '36741726'},
                             {'averageRecordSize': '319',
                              'name': 'albuquerque_nm_06',
                              'recordCount': '161739',
                              'sizeKey': '51725911'},
                             {'averageRecordSize': '293',
                              'name': 'dover_de_09',
                              'recordCount': '26328',
                              'sizeKey': '7739615'},
                             {'averageRecordSize': '326',
                              'name': 'houston_tx_10',
                              'recordCount': '540904',
                              'sizeKey': '176348274'},
                             {'averageRecordSize': '330',
                              'name': 'oklahomacity_ok_13',
                              'recordCount': '270013',
                              'sizeKey': '89307226'},
                             {'averageRecordSize': '264',
                              'name': 'richmond_va_08',
                              'recordCount': '51997',
                              'sizeKey': '13780344'},
                             {'averageRecordSize': '288',
                              'name': 'salem_or_08',
                              'recordCount': '83822',
                              'sizeKey': '24180996'},
                             {'averageRecordSize': '314',
                              'name': 'mobile_al_10',
                              'recordCount': '12336',
                              'sizeKey': '3884398'},
                             {'averageRecordSize': '309',
                              'name': 'denver_co_12',
                              'recordCount': '734236',
                              'sizeKey': '227563240'},
                             {'averageRecordSize': '271',
                              'name': 'birmingham_al_08',
                              'recordCount': '76303',
                              'sizeKey': '20708237'},
                             {'averageRecordSize': '265',
                              'name': 'milwaukee_wi_07',
                              'recordCount': '241337',
                              'sizeKey': '64057224'},
                             {'averageRecordSize': '315',
                              'name': 'baltimore_md_08',
                              'recordCount': '145123',
                              'sizeKey': '45735439'},
                             {'averageRecordSize': '295',
                              'name': 'concord_nh_09',
                              'recordCount': '21718',
                              'sizeKey': '6431987'},
                             {'averageRecordSize': '275',
                              'name': 'dayton_oh_06',
                              'recordCount': '131003',
                              'sizeKey': '36075259'},
                             {'averageRecordSize': '281',
                              'name': 'jeffersoncity_mo_08',
                              'recordCount': '30593',
                              'sizeKey': '8621651'},
                             {'averageRecordSize': '302',
                              'name': 'tulsa_ok_08',
                              'recordCount': '238526',
                              'sizeKey': '72251388'},
                             {'averageRecordSize': '378',
                              'name': 'albuquerque_nm_12',
                              'recordCount': '162952',
                              'sizeKey': '61686877'},
                             {'averageRecordSize': '492',
                              'name': 'newyork_ny_05',
                              'recordCount': '141559',
                              'sizeKey': '69741096'},
                             {'averageRecordSize': '335',
                              'name': 'fresno_ca_13',
                              'recordCount': '212227',
                              'sizeKey': '71294022'},
                             {'averageRecordSize': '307',
                              'name': 'montgomery_al_07',
                              'recordCount': '74633',
                              'sizeKey': '22989911'},
                             {'averageRecordSize': '257',
                              'name': 'scranton_pa_08',
                              'recordCount': '94092',
                              'sizeKey': '24235638'},
                             {'averageRecordSize': '292',
                              'name': 'wichita_ks_12',
                              'recordCount': '115740',
                              'sizeKey': '33894037'},
                             {'averageRecordSize': '330',
                              'name': 'atlanta_ga_13',
                              'recordCount': '462468',
                              'sizeKey': '152920479'},
                             {'averageRecordSize': '279',
                              'name': 'norfolk_va_07',
                              'recordCount': '364694',
                              'sizeKey': '101947536'},
                             {'averageRecordSize': '291',
                              'name': 'albany_ny_06',
                              'recordCount': '111078',
                              'sizeKey': '32408514'},
                             {'averageRecordSize': '276',
                              'name': 'lincoln_ne_08',
                              'recordCount': '90377',
                              'sizeKey': '24952120'},
                             {'averageRecordSize': '291',
                              'name': 'oklahomacity_ok_07',
                              'recordCount': '148648',
                              'sizeKey': '43372195'},
                             {'averageRecordSize': '300',
                              'name': 'madison_wi_10',
                              'recordCount': '90018',
                              'sizeKey': '27098362'},
                             {'averageRecordSize': '273',
                              'name': 'columbus_oh_12',
                              'recordCount': '288711',
                              'sizeKey': '78900992'},
                             {'averageRecordSize': '282',
                              'name': 'portland_or_12',
                              'recordCount': '279024',
                              'sizeKey': '78774591'},
                             {'averageRecordSize': '331',
                              'name': 'anaheim_ca_10',
                              'recordCount': '28352',
                              'sizeKey': '9390601'},
                             {'averageRecordSize': '318',
                              'name': 'hartford_ct_13',
                              'recordCount': '126818',
                              'sizeKey': '40414690'}]

        try:
            logger.info(f"Recreating table: {self.pv_rooftops_buildings_table_name}")
            response = self.glue.delete_table(
                DatabaseName=self.database_name,
                Name=self.pv_rooftops_buildings_table_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.debug(r"Skipping table deletion as it does not exist: {self.pv_rooftops_buildings_table_name}")

        logger.info("Creating pv_rooftops buildings table")

        response = self.glue.create_table(
            DatabaseName=self.database_name,
            TableInput={
                'Name': self.pv_rooftops_buildings_table_name,
                'PartitionKeys': [{'Name': 'city_year', 'Type': 'string'}],
                'Retention': 0,
                'Parameters': {'averageRecordSize': '271',
                               'classification': 'parquet',
                               'compressionType': 'none',
                               'objectCount': '168',
                               'recordCount': '35097591',
                               'sizeKey': '10417280616',
                               'typeOfData': 'file'},
                'StorageDescriptor': {'BucketColumns': [],
                                 'Columns': [{'Name': 'gid', 'Type': 'bigint'},
                                             {'Name': 'bldg_fid',
                                              'Type': 'bigint'},
                                             {'Name': 'the_geom_96703',
                                              'Type': 'string'},
                                             {'Name': 'the_geom_4326',
                                              'Type': 'string'},
                                             {'Name': 'city', 'Type': 'string'},
                                             {'Name': 'state',
                                              'Type': 'string'},
                                             {'Name': 'year', 'Type': 'bigint'},
                                             {'Name': 'region_id',
                                              'Type': 'bigint'},
                                             {'Name': '__index_level_0__',
                                              'Type': 'bigint'}],
                                 'Compressed': False,
                                 'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                                 'Location': f'{self.pv_rooftops_bucket}/buildings/',
                                 'NumberOfBuckets': -1,
                                 'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                                 'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0',
                                                'CrawlerSchemaSerializerVersion': '1.0',
                                                'UPDATED_BY_CRAWLER': 'pv_rooftop_buildings',
                                                'averageRecordSize': '271',
                                                'classification': 'parquet',
                                                'compressionType': 'none',
                                                'objectCount': '168',
                                                'recordCount': '35097591',
                                                'sizeKey': '10417280616',
                                                'typeOfData': 'file'},
                                 'SerdeInfo': {'Parameters': {'serialization.format': '1'},
                                               'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'},
                                 'SortColumns': [],
                                 'StoredAsSubDirectories': False},
           'TableType': 'EXTERNAL_TABLE'
            }
        )

        #pprint(response)

        logger.info("Creating pv_rooftops buildings partitions")

        count = 0
        templateArray = []
        for partition in partitionsArray:
            #pprint(partition)
            count = count + 1
            thisTemplate = self.get_pv_rooftop_bldg_partition_template()
            thisTemplate['StorageDescriptor']['Location'] = f"{self.pv_rooftops_bucket}/buildings/{partition['name']}/"
            thisTemplate['StorageDescriptor']['Parameters']['averageRecordSize'] = partition['averageRecordSize']
            thisTemplate['StorageDescriptor']['Parameters']['recordCount'] = partition['recordCount']
            thisTemplate['StorageDescriptor']['Parameters']['sizeKey'] = partition['sizeKey']
            thisTemplate['Values'][0] = partition['name']
            templateArray.append(thisTemplate)

            #pprint(templateArray)
            #print(count)
            if count%10 == 0:
                #print('submitting')
                logger.info("Submitting pv_rooftop building partitions batch")
                #pprint(templateArray)
                response2 = self.glue.batch_create_partition(
                    DatabaseName = self.database_name,
                    TableName = self.pv_rooftops_buildings_table_name,
                    PartitionInputList = templateArray
                )
                #pprint(response2)
                templateArray = []

        if count%10 != 0:
            logger.info("Submitting pv_rooftop building partitions batch")
            response2 = self.glue.batch_create_partition(
                DatabaseName=self.database_name,
                TableName=self.pv_rooftops_buildings_table_name,
                PartitionInputList=templateArray
            )
            #pprint(response2)

