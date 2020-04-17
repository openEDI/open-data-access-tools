#!/usr/bin/env python3

from aws_cdk import core

from oedi.config import data_lake_config
from oedi.AWS.data_lake.stack import DataLakeStack


LOGO = """

,---.                   ,---.                             ,--.      |             |     o|    o     |    o           
|   |,---.,---.,---.    |--- ,---.,---.,---.,---.,   .    |   |,---.|--- ,---.    |,---..|--- .,---.|--- ..    ,,---.
|   ||   ||---'|   |    |    |   ||---'|    |   ||   |    |   |,---||    ,---|    ||   |||    |,---||    | \  / |---'
`---'|---'`---'`   '    `---'`   '`---'`    `---|`---|    `--' `---^`---'`---^    ``   '``---'``---^`---'`  `'  `---'
     |                                      `---'`---'                                                                
"""

print(LOGO)

app = core.App()

data_lake_name = data_lake_config.data_lake_name
aws_region = data_lake_config.aws_region
DataLakeStack(app, data_lake_name, env={"region": aws_region})

app.synth()
