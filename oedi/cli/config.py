import click

from oedi.config import AWSDataLakeConfig

CLOUD_PROVIDERS = ["AWS"]


@click.group()
def config():
    """OEDI configurations of each cloud provider."""
    pass


@click.command()
@click.option(
    "-p", "--provider",
    type=click.Choice(CLOUD_PROVIDERS, case_sensitive=False),
    required=True,
    help="Show the configuration of OEDI data lake provider."
)
def show(provider):
    provider = str(provider).upper()
    print(f"OEDI {provider.upper()} Config:")
    print("------------")
    
    if provider == "AWS":
        config = AWSDataLakeConfig()
    else:
        config = None
    template = config.to_string()
    print(template)

config.add_command(show)
