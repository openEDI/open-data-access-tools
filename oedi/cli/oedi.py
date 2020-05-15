import time
import click

from oedi.cli.athena import run_query
from oedi.cli.config import show_config
from oedi.cli.glue import (
    list_databases,
    list_tables,
    list_crawlers, 
    run_crawler, 
    run_crawlers
)


@click.group()
def cli():
    pass

cli.add_command(list_databases)
cli.add_command(list_tables)
cli.add_command(run_query)
cli.add_command(show_config)
cli.add_command(list_crawlers)
cli.add_command(run_crawler)
cli.add_command(run_crawlers)
