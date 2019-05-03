import yaml
import boto3
import logging
import logging.config
import os
import argparse

from providers.AWS.AwsGlue import AwsGlue
from providers.AWS.AwsBase import AwsBase

logger = logging.getLogger("OEDI")

print(__name__)

#logging.getLogger().setLevel(logging.DEBUG)
#logging.basicConfig(level=logging.DEBUG)

class OEDI():
    LOGO = '''

    ,---.                   ,---.                             ,--.      |             |     o|    o     |    o           
    |   |,---.,---.,---.    |--- ,---.,---.,---.,---.,   .    |   |,---.|--- ,---.    |,---..|--- .,---.|--- ..    ,,---.
    |   ||   ||---'|   |    |    |   ||---'|    |   ||   |    |   |,---||    ,---|    ||   |||    |,---||    | \  / |---'
    `---'|---'`---'`   '    `---'`   '`---'`    `---|`---|    `--' `---^`---'`---^    ``   '``---'``---^`---'`  `'  `---'
         |                                      `---'`---'                                                                
        '''

    def __init__(self, configuration_file):
        self.project_filename = os.path.abspath(configuration_file)

        with open(self.project_filename, 'r') as f:
            self.cfg = yaml.load(f)

        #print(self.cfg.keys())
        #print(self.cfg['aws'].keys())
        if 'oedi_database_name' not in self.cfg['aws'].keys():
            raise KeyError('Key `oedi_database_name` not specified in project file `{}`'.format(configuration_file))
        if 'datasets_to_include' not in self.cfg['aws'].keys():
            raise KeyError('Key `datasets_to_include` not specified in project file `{}`'.format(configuration_file))

        self.oedi_database_name = self.cfg['aws']['oedi_database_name']
        self.region = self.cfg['aws']['region']
        # shared session object:
        self.boto3_session = boto3.Session(region_name=self.region)

    def build_catalog(self):

        glue = AwsGlue(self.boto3_session, self.oedi_database_name)
        glue.create_database()
        glue.create_tracking_the_sun_table()
        glue.create_pv_rooftop_buildings_table()



    def clean(self):
        print('cleaning up')


if __name__ == '__main__':
    print('logconfig')
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
            'OEDI': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            }
        },
    })

    print(OEDI.LOGO)

    parser = argparse.ArgumentParser()
    parser.add_argument('configuration_file')
    parser.add_argument('-c', '--clean', action='store_true')
    args = parser.parse_args()
    oedi = OEDI(args.configuration_file)
    if args.clean == True:
        oedi.clean()
    else:
        oedi.build_catalog()
