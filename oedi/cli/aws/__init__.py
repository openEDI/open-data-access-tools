import click

from oedi.cli.AWS import athena, glue


@click.group()
def aws():
    """OEDI command with AWS cloud."""
    pass


aws.add_command(glue.list_databases)
aws.add_command(glue.list_tables)
aws.add_command(glue.list_crawlers)
aws.add_command(glue.run_crawler)
aws.add_command(glue.run_crawlers)
aws.add_command(athena.run_query)