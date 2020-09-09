#!/usr/bin/env python3

from aws_cdk import core
from oedi.AWS.data_lake import AWSDataLakeStack

LOGO = """

,---.                   ,---.                             ,--.      |             |     o|    o     |    o           
|   |,---.,---.,---.    |--- ,---.,---.,---.,---.,   .    |   |,---.|--- ,---.    |,---..|--- .,---.|--- ..    ,,---.
|   ||   ||---'|   |    |    |   ||---'|    |   ||   |    |   |,---||    ,---|    ||   |||    |,---||    | \  / |---'
`---'|---'`---'`   '    `---'`   '`---'`    `---|`---|    `--' `---^`---'`---^    ``   '``---'``---^`---'`  `'  `---'
     |                                      `---'`---'                                                                
"""

print(LOGO)

app = core.App()
AWSDataLakeStack(app)
app.synth()
