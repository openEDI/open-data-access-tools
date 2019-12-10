#!/usr/bin/env python3

from aws_cdk import core

from data_lake.data_lake_stack import DataLakeStack

app = core.App()
DataLakeStack(app, "data-lake", env={'account': '246460460343', 'region': 'us-west-2' })

app.synth()

