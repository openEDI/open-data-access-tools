import yaml
import boto3
import logging
import logging.config
import os
import argparse
from AwsGlue import AwsGlue
from AwsBase import AwsBase

logger = logging.getLogger(__name__)

class OEDI(AwsBase):

    def __init__(self, configuration_file):
        super().__init__(configuration_file)
        self.oedi_database_name = self.cfg['oedi_database_name']
        self.region = self.cfg['aws']['region']
        # shared session object:
        self.boto3_session = boto3.Session(profile_name='oedi', region_name=self.region)

    def build_catalog(self):

        glue = AwsGlue(self.boto3_session, self.oedi_database_name)
        glue.create_database()
        glue.create_tracking_the_sun_table()

    def clean(self):
        print('cleaning up')


if __name__ == '__main__':
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'defaultfmt': {
                'format': '%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'defaultfmt',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            }
        },
        'loggers': {
            '__main__': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            },
            'buildstockbatch': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            }
        },
    })

    print(AwsBase.LOGO)

    parser = argparse.ArgumentParser()
    parser.add_argument('configuration_file')
    parser.add_argument('-c', '--clean', action='store_true')
    args = parser.parse_args()
    oedi = OEDI(args.configuration_file)
    if args.clean == True:
        oedi.clean()
    else:
        oedi.build_catalog()
