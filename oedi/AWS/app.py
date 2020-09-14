#!/usr/bin/env python3

from aws_cdk import core

from oedi.config import AWSDataLakeConfig, OEDI_CONFIG_FILLE
from oedi.AWS.data_lake.stack import AWSDataLakeStack

LOGO = """

,---.                   ,---.                             ,--.      |             |     o|    o     |    o           
|   |,---.,---.,---.    |--- ,---.,---.,---.,---.,   .    |   |,---.|--- ,---.    |,---..|--- .,---.|--- ..    ,,---.
|   ||   ||---'|   |    |    |   ||---'|    |   ||   |    |   |,---||    ,---|    ||   |||    |,---||    | \  / |---'
`---'|---'`---'`   '    `---'`   '`---'`    `---|`---|    `--' `---^`---'`---^    ``   '``---'``---^`---'`  `'  `---'
     |                                      `---'`---'                                                                
"""

print(LOGO)

app = core.App()

config = AWSDataLakeConfig(OEDI_CONFIG_FILLE)
AWSDataLakeStack(app, env={"region": config.region_name})
app.synth()
