import click

from oedi.config import data_lake_config

__all__ = ["show_config"]


@click.command()
def show_config():
    """Show data lake configuration."""
    template = data_lake_config.to_string()
    print(template)
