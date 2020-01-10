#!/usr/bin/env python3

from aws_cdk import core

from data_lake.data_lake_stack import DataLakeStack

LOGO = '''

,---.                   ,---.                             ,--.      |             |     o|    o     |    o           
|   |,---.,---.,---.    |--- ,---.,---.,---.,---.,   .    |   |,---.|--- ,---.    |,---..|--- .,---.|--- ..    ,,---.
|   ||   ||---'|   |    |    |   ||---'|    |   ||   |    |   |,---||    ,---|    ||   |||    |,---||    | \  / |---'
`---'|---'`---'`   '    `---'`   '`---'`    `---|`---|    `--' `---^`---'`---^    ``   '``---'``---^`---'`  `'  `---'
     |                                      `---'`---'                                                                
    '''

print(LOGO)

app = core.App()
DataLakeStack(app, "oedidatalake", env={'region': 'us-west-2' })

app.synth()

