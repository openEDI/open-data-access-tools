import time
import click

from oedi.config import data_lake_config
from oedi.AWS.utils.glue import (
    list_available_crawlers,
    start_crawler,
    check_crawler_state,
    list_available_tables
)


@click.command()
def list_crawlers():
    """List available crawlers."""
    crawlers = list_available_crawlers()

    if not crawlers:
        print(None)

    for crawler in crawlers:
        print(" - " + crawler)


@click.command()
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


@click.command()
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


@click.command()
def list_tables():
    """List available tables."""
    tables = list_available_tables(
        database_name=data_lake_config.database_name
    )
    
    for table in tables:
        print(" - " + table)
