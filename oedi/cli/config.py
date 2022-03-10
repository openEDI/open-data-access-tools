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
def sync():
    """Sync OEDI configurations of cloud providers."""
    oedi_defalt_config_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config.yaml"
    )

    os.makedirs(OEDI_CONFIG_DIR, exist_ok=True)

    if not os.path.exists(OEDI_CONFIG_FILE):
        shutil.copyfile(oedi_defalt_config_file, OEDI_CONFIG_FILE)
        print(f"OEDI configuration synced @ {OEDI_CONFIG_FILE}")
        print("Please edit the file and provide staging location for AWS data lake.")
        return
    
    # Update AWS data lake config
    default_config = AWSDataLakeConfig(oedi_defalt_config_file)
    local_config = AWSDataLakeConfig(OEDI_CONFIG_FILE)
    local_config.update(data=default_config.data)

    print(f"OEDI configuration synced @ {OEDI_CONFIG_FILE}")


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


config.add_command(sync)
config.add_command(show)
