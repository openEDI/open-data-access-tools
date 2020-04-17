import time
import click

from oedi.config import data_lake_config
from oedi.AWS.utils.glue import (
    list_available_crawlers,
    start_crawler,
    check_crawler_state,
    list_available_tables
)
from oedi.AWS.utils.athena import run_pyathena


@click.group()
def cli():
    pass


@cli.command()
def show_config():
    """Show data lake configuration."""
    template = data_lake_config.to_string()
    print(template)


@cli.command()
def list_crawlers():
    """List available crawlers."""
    crawlers = list_available_crawlers()

    if not crawlers:
        print(None)

    for crawler in crawlers:
        print(" - " + crawler)


@cli.command()
@click.option(
    "-n", "--crawler-name",
    type=click.STRING,
    required=True,
    help="The crawler name in OEDI data lake."
)
@click.option(
    "-b", "--background-run",
    type=click.BOOL,
    is_flag=True,
    default=False,
    show_default=True,
    help="Run crawler in background."
)
def run_crawler(crawler_name, background_run=False):
    """Run crawler to populate table."""
    state = check_crawler_state(crawler_name)
    if state == "READY":
        print("Starting crawler...")
        start_crawler(crawler_name)
    else:
        print(f"Crawler has already started. State={state.lower()}...")
        return
    
    if background_run:
        print("Started!")
        return
    
    running_started, stopping_started = False, False
    while True:
        time.sleep(1)
        state = check_crawler_state(crawler_name)
        
        if state == "RUNNING" and not running_started:
            print(f"Running crawler...")
            running_started = True
            continue
        
        if state == "STOPPING" and not stopping_started:
            print(f"Stopping crawler...")
            stopping_started = True
            continue
        
        if state == "READY":
            print("Finished!")
            break


@cli.command()
def run_crawlers():
    """Run all crawlers in data lake."""
    crawlers = list_available_crawlers()
    
    for crawler_name in crawlers:
        state = check_crawler_state(crawler_name)
        if state == "READY":
            print(f"Starting crawler {crawler_name}...")
            start_crawler(crawler_name)
        else:
            print(f"Crawler '{crawler_name}' has already started. State={state.lower()}...")
            continue
    
    print("All crawlers started!")


@cli.command()
def list_tables():
    """List available tables."""
    tables = list_available_tables(
        database_name=data_lake_config.database_name
    )
    
    for table in tables:
        print(" - " + table)


@cli.command()
@click.option(
    "-q", "--query-string",
    type=click.STRING,
    required=True,
    help="Valid SQL query string."
)
@click.option(
    "-o", "--output-location",
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
    "--head",
    type=click.BOOL,
    is_flag=True,
    default=False,
    show_default=True,
    help="Show pandas DataFrame head only."
)
def run_query(query_string, output_location=None, region_name=None, head=False):
    """Run SQL query and show result."""
    region_name = data_lake_config.aws_region
    if not output_location:
        output_location = data_lake_config.output_location
    
    # The user may not configure Output Location in config
    if not output_location:
        raise ValueError("Invalid '--output-location' option value.")
    
    result = run_pyathena(
        query_string=query_string,
        s3_staging_dir=output_location,
        region_name=region_name
    )
    
    if head:
        print(result.head())
        return
    
    print(result)
