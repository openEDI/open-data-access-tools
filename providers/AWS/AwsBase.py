import yaml
import os
from pprint import pprint

class AwsBase(object):
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
        if 'oedi_database_name' not in self.cfg.keys():
            raise KeyError('Key `oedi_database_name` not specified in project file `{}`'.format(configuration_file))
        if 'datasets_to_include' not in self.cfg.keys():
            raise KeyError('Key `datasets_to_include` not specified in project file `{}`'.format(configuration_file))
