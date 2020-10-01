"""
Deploy OEDI data lake before running the integration tests.

To deploy OEDI data lake, run the commands below:
$ oedi init
$ cd oedi/AWS
$ cdk deploy
"""
import pytest

from oedi.config import AWSDataLakeConfig
from oedi.AWS.athena import OEDIAthena
from oedi.AWS.glue import OEDIGlue


def test_lbnl_tracking_the_sun_2018():
    # config
    config = AWSDataLakeConfig()
    database_name = config.database_name
    table_name = "lbnl_tracking_the_sun_2018"
    
    # Partitions
    glue = OEDIGlue()
    partition_keys = glue.get_partition_keys(database_name, table_name, with_pandas=False)
    assert len(partition_keys) == 1
    assert partition_keys[0]["Name"] == "state"
    
    partition_values = glue.get_partition_values(database_name, table_name)
    if "__HIVE_DEFAULT_PARTITION__" in partition_values:
        partition_values.remove("__HIVE_DEFAULT_PARTITION__")
    assert len(partition_values) == 25
    
    # Query
    athena = OEDIAthena(
        staging_location=config.staging_location,
        region_name=config.region_name
    )
    sql = f"SELECT COUNT(*) FROM {database_name}.{table_name} WHERE state='CO';"
    result = athena.run_query(query_string=sql, pandas_cursor=False)
    assert result[0][0] == 40714
    
    sql = f"SELECT * FROM {database_name}.{table_name} WHERE state='CO' AND system_id_from_data_provider='SRO00164';"
    df = athena.run_query(query_string=sql, pandas_cursor=True)
    assert df.shape == (1, 63)
    assert pytest.approx(df["total_installed_price"].values[0]) == 22504.46


def test_lbnl_tracking_the_sun_2019():
    # config
    config = AWSDataLakeConfig()
    database_name = config.database_name
    table_name = "lbnl_tracking_the_sun_2019"
    
    # Partitions
    glue = OEDIGlue()
    partition_keys = glue.get_partition_keys(database_name, table_name, with_pandas=False)
    assert len(partition_keys) == 1
    assert partition_keys[0]["Name"] == "state"
    
    partition_values = glue.get_partition_values(database_name, table_name)
    if "__HIVE_DEFAULT_PARTITION__" in partition_values:
        partition_values.remove("__HIVE_DEFAULT_PARTITION__")
    assert len(partition_values) == 28
    
    # Query
    athena = OEDIAthena(
        staging_location=config.staging_location,
        region_name=config.region_name
    )
    sql = f"SELECT COUNT(*) FROM {database_name}.{table_name} WHERE state='CO';"
    result = athena.run_query(query_string=sql, pandas_cursor=False)
    assert result[0][0] == 93930
    
    sql = f"SELECT * FROM {database_name}.{table_name} WHERE state='CO' AND system_id_from_first_data_provider='SRO00164';"
    df = athena.run_query(query_string=sql, pandas_cursor=True)
    assert df.shape == (2, 60)
    assert pytest.approx(df["total_installed_price"].values[0]) == 22504.46


def test_nrel_pv_rooftops_aspects():
    # config
    config = AWSDataLakeConfig()
    database_name = config.database_name
    table_name = "nrel_pv_rooftops_aspects"
    
    # Partitions
    glue = OEDIGlue()
    partition_keys = glue.get_partition_keys(database_name, table_name, with_pandas=False)
    assert len(partition_keys) == 1
    assert partition_keys[0]["Name"] == "partition_0"
    
    partition_values = glue.get_partition_values(database_name, table_name)
    if "__HIVE_DEFAULT_PARTITION__" in partition_values:
        partition_values.remove("__HIVE_DEFAULT_PARTITION__")
    assert len(partition_values) == 165
    
    # Query
    athena = OEDIAthena(
        staging_location=config.staging_location,
        region_name=config.region_name
    )
    sql = f"SELECT COUNT(*) FROM {database_name}.{table_name} WHERE partition_0='augusta_me_08';"
    result = athena.run_query(query_string=sql, pandas_cursor=False)
    assert result[0][0] == 197547
    
    sql = f"SELECT * FROM {database_name}.{table_name} WHERE partition_0='augusta_me_08' AND gid=53283;"
    df = athena.run_query(query_string=sql, pandas_cursor=True)
    assert df.shape == (1, 11)
    assert df["aspect"].values[0] == 7


def test_nrel_pv_rooftops_buildings():
    # config
    config = AWSDataLakeConfig()
    database_name = config.database_name
    table_name = "nrel_pv_rooftops_buildings"
    
    # Partitions
    glue = OEDIGlue()
    partition_keys = glue.get_partition_keys(database_name, table_name, with_pandas=False)
    assert len(partition_keys) == 1
    assert partition_keys[0]["Name"] == "partition_0"
    
    partition_values = glue.get_partition_values(database_name, table_name)
    if "__HIVE_DEFAULT_PARTITION__" in partition_values:
        partition_values.remove("__HIVE_DEFAULT_PARTITION__")
    assert len(partition_values) == 168
    
    # Query
    athena = OEDIAthena(
        staging_location=config.staging_location,
        region_name=config.region_name
    )
    sql = f"SELECT COUNT(*) FROM {database_name}.{table_name} WHERE partition_0='augusta_me_08';"
    result = athena.run_query(query_string=sql, pandas_cursor=False)
    assert result[0][0] == 17431
    
    sql = f"SELECT * FROM {database_name}.{table_name} WHERE partition_0='augusta_me_08' AND gid=16039;"
    df = athena.run_query(query_string=sql, pandas_cursor=True)
    assert df.shape == (1, 10)
    assert df["the_geom_4326"].values[0].startswith("MULTIPOLYGON(((-69.7528548573896 44.3860361743938,")


def test_nrel_pv_rooftops_developable_planes():
    # config
    config = AWSDataLakeConfig()
    database_name = config.database_name
    table_name = "nrel_pv_rooftops_developable_planes"
    
    # Partitions
    glue = OEDIGlue()
    partition_keys = glue.get_partition_keys(database_name, table_name, with_pandas=False)
    assert len(partition_keys) == 1
    assert partition_keys[0]["Name"] == "partition_0"
    
    partition_values = glue.get_partition_values(database_name, table_name)
    if "__HIVE_DEFAULT_PARTITION__" in partition_values:
        partition_values.remove("__HIVE_DEFAULT_PARTITION__")
    assert len(partition_values) == 166
    
    # Query
    athena = OEDIAthena(
        staging_location=config.staging_location,
        region_name=config.region_name
    )
    sql = f"SELECT COUNT(*) FROM {database_name}.{table_name} WHERE partition_0='topeka_ks_08';"
    result = athena.run_query(query_string=sql, pandas_cursor=False)
    assert result[0][0] == 537304
    
    sql = f"SELECT * FROM {database_name}.{table_name} WHERE partition_0='augusta_me_08' AND gid=15924;"
    df = athena.run_query(query_string=sql, pandas_cursor=True)
    assert df.shape == (1, 18)
    assert df["slope"].values[0] == 26


def test_nrel_pv_rooftops_rasd():
    # config
    config = AWSDataLakeConfig()
    database_name = config.database_name
    table_name = "nrel_pv_rooftops_rasd"
    
    # Partitions
    glue = OEDIGlue()
    partition_keys = glue.get_partition_keys(database_name, table_name, with_pandas=False)
    assert len(partition_keys) == 1
    assert partition_keys[0]["Name"] == "partition_0"
    
    partition_values = glue.get_partition_values(database_name, table_name)
    if "__HIVE_DEFAULT_PARTITION__" in partition_values:
        partition_values.remove("__HIVE_DEFAULT_PARTITION__")
    assert len(partition_values) == 166
    
    # Query
    athena = OEDIAthena(
        staging_location=config.staging_location,
        region_name=config.region_name
    )
    sql = f"SELECT COUNT(*) FROM {database_name}.{table_name} WHERE partition_0='topeka_ks_08';"
    result = athena.run_query(query_string=sql, pandas_cursor=False)
    assert result[0][0] == 1
    
    sql = f"SELECT * FROM {database_name}.{table_name} WHERE partition_0='augusta_me_08'"
    df = athena.run_query(query_string=sql, pandas_cursor=True)
    assert df.shape == (1, 10)
    assert df["the_geom_96703"].values[0].startswith("MULTIPOLYGON Z (((2059331.26461854 2664063.99720305 86.4199981689453,")
