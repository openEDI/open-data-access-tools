import click

from oedi.config import AWSDataLakeConfig
from oedi.AWS.athena import OEDIAthena


@click.command()
@click.option(
    "-q", "--query-string",
    type=click.STRING,
    required=True,
    help="Valid SQL query string."
)
@click.option(
    "-s", "--staging-location",
    type=click.STRING,
    required=False,
    default=None,
    help="A S3 staging directory."
)
@click.option(
    "-r", "--region-name",
    type=click.STRING,
    required=False,
    default=None,
    help="AWS region name, i.e. us-west-2"
)
@click.option(
    "-o",
    "--output-file",
    type=click.Path(),
    default=None,
    help="Export result to CSV file."
)
@click.option(
    "--head",
    type=click.BOOL,
    is_flag=True,
    default=False,
    show_default=True,
    help="Show pandas DataFrame head only."
)
def run_query(query_string, staging_location=None, region_name=None, output_file=None, head=False):
    """Run SQL query and show/export result."""
    config = AWSDataLakeConfig()
    region_name = region_name or config.region_name
    if not staging_location:
        staging_location = config.staging_location

    # The user may not configure Staging Location in config
    if not staging_location:
        raise ValueError("Invalid '--output-location' option value.")

    oedi_athena = OEDIAthena(staging_location, region_name)
    result = oedi_athena.run_query(query_string)
    if head:
        result = result.head()

    if output_file:
        result.to_csv(output_file, index=False)
        print(f"Exprted query result to csv file - {output_file}.")
        return

    print(result)
