import time

import click
from prettytable import PrettyTable

from oedi.config import AWSDataLakeConfig
from oedi.AWS.glue import OEDIGlue

glue = OEDIGlue()


@click.command()
def list_databases():
    """List available databases"""
    databases = glue.get_databases()
    
    pretty_table = PrettyTable()
    pretty_table.field_names = ["No.", "Name", "CreateTime"]
    for i, db in enumerate(databases):
        pretty_table.add_row([i, db["Name"], db["CreateTime"]])

    print("All available databaes are:")
    print(pretty_table)


@click.command()
@click.option(
    "-d", "--database-name",
    type=click.STRING,
    required=False,
    default=None,
    help="List the tables in Glue database."
)
def list_tables(database_name):
    """List available tables in database."""
    if not database_name:
        config = AWSDataLakeConfig()
        database_name = config.database_name
    tables = glue.list_tables(
        database_name=database_name
    )
    pretty_table = PrettyTable()
    pretty_table.field_names = ["No.", "Name", "CreateTime"]
    for i, table in enumerate(tables):
        pretty_table.add_row([i, table["Name"], table["CreateTime"]])

    print(f"All available tables in [{database_name}] are:")
    print(pretty_table)


@click.command()
def list_crawlers():
    """List available crawlers."""
    crawlers = glue.list_crawlers()

    pretty_table = PrettyTable()
    pretty_table.field_names = ["No.", "Name", "State", "S3Targets", "LastUpdated", "CreateTime"]
    for i, crawler in enumerate(crawlers):
        pretty_table.add_row([
            i, crawler["Name"], crawler["State"], crawler["S3Targets"], 
            crawler["LastUpdated"], crawler["CreateTime"]
        ])

    print("All availables crawlers are:")
    print(pretty_table)


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
    state = glue.get_crawler_state(crawler_name)
    if state == "READY":
        print("Starting crawler...")
        glue.start_crawler(crawler_name)
    else:
        print(f"Crawler has already started. State={state.lower()}...")
        return
    
    if background_run:
        print("Started!")
        return
    
    running_started, stopping_started = False, False
    while True:
        time.sleep(1)
        state = glue.get_crawler_state(crawler_name)
        
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
    crawlers = glue.list_crawlers()
    #print(crawlers)
    for crawler_name in crawlers:
        #print(crawler_name)
        state = glue.get_crawler_state(crawler_name['Name'])
        if state == "READY":
            print(f"Starting crawler {crawler_name['Name']}...")
            glue.start_crawler(crawler_name['Name'])
        else:
            print(f"Crawler '{crawler_name['Name']}' has already started. State={state.lower()}...")
            continue
    
    print("All crawlers started!")
