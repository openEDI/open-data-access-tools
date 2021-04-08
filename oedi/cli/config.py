import click

from oedi.config import init_config, AWSDataLakeConfig

OEDI_CLOUD_PROVIDERS = ["AWS"]


@click.group()
def config():
    """OEDI configurations of each cloud provider."""
    pass


@click.command()
def init():
    """Init OEDI configurations of cloud providers."""
    init_config()


@click.command()
@click.option(
    "-p", "--provider",
    type=click.Choice(OEDI_CLOUD_PROVIDERS, case_sensitive=False),
    required=True,
    help="Show the configuration of OEDI data lake provider."
)
def show(provider):
    """Show OEDI configuration of given cloud provider."""
    provider = str(provider).upper()
    print(provider.upper())
    print("-" * len(provider))

    if provider == "AWS":
        config = AWSDataLakeConfig()

    template = config.to_string()
    print(template)


config.add_command(init)
config.add_command(show)
