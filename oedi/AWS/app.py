#!/usr/bin/env python3

from aws_cdk import core

from oedi.config import AWSDataLakeConfig, OEDI_CONFIG_FILE
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
config = AWSDataLakeConfig(OEDI_CONFIG_FILE)
AWSDataLakeStack(app, config)
app.synth()
