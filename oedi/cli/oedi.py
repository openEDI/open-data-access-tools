import click

from oedi.cli.config import config
from oedi.cli.AWS import aws


@click.group()
def cli():
    pass


cli.add_command(config)
cli.add_command(aws)
