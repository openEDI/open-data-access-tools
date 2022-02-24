import os
import shutil

import click

from oedi.config import OEDI_CONFIG_FILE, OEDI_CONFIG_DIR, AWSDataLakeConfig

OEDI_CLOUD_PROVIDERS = ["AWS"]


@click.group()
def config():
    """OEDI configurations of each cloud provider."""
    pass


@click.command()
@click.option(
    "-p", "--provider",
    type=click.Choice(OEDI_CLOUD_PROVIDERS, case_sensitive=False),
    default=None,
    help="Setup configuration with given cloud provider."
)
def setup(provider):
    """Setup OEDI configurations of cloud providers."""
    if provider:
        provider = str(provider).upper()
    oedi_defalt_config_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config.yaml"
    )

    os.makedirs(OEDI_CONFIG_DIR, exist_ok=True)
    if not provider or not os.path.exists(OEDI_CONFIG_FILE):
        shutil.copyfile(oedi_defalt_config_file, OEDI_CONFIG_FILE)
    
    elif os.path.exists(OEDI_CONFIG_FILE) and provider == "AWS":
        default_config = AWSDataLakeConfig(oedi_defalt_config_file)
        local_config = AWSDataLakeConfig(OEDI_CONFIG_FILE)
        breakpoint()
        local_config.update(data=default_config.data)

    print(f"OEDI configuration setup @ {OEDI_CONFIG_FILE}")


@click.command()
@click.option(
    "-p", "--provider",
    type=click.Choice(OEDI_CLOUD_PROVIDERS, case_sensitive=False),
    required=True,
    help="Show the configuration of given cloud provider."
)
def show(provider):
    """Show OEDI configuration of given cloud provider."""
    provider = str(provider).upper()
    if provider == "AWS":
        config = AWSDataLakeConfig()

    template = config.to_string()
    print(template)


config.add_command(setup)
config.add_command(show)
