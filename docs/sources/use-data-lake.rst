Use Data Lake
=============

Populate Tables
---------------
The prior steps created a database ``oedi_database`` in AWS Glue, but it's empty. Now, we need 
to run the crawlers to populate the tables in the database. 

In your ``oedi`` environment, navigate back to the main directory, ``open-data-access-tools``, and use the commands below to check and run the glue crawlers.

1. List Crawlers

.. code-block:: bash

    (oedi) $ oedi aws list-crawlers
    All availables crawlers are:
    +-----+-------------------------------------------------------------------+-------+------------------------------------------------------------------------+---------------------------+---------------------------+
    | No. |                                Name                               | State |                               S3Targets                                |        LastUpdated        |         CreateTime        |
    +-----+-------------------------------------------------------------------+-------+------------------------------------------------------------------------+---------------------------+---------------------------+
    |  0  | nrel-pds-building-stock-comstock-athena-2020-comstock-v1-metadata | READY | s3://nrel-pds-building-stock/comstock/athena/2020/comstock_v1/metadata | 2022-01-09 23:28:24+00:00 | 2022-01-09 23:28:24+00:00 |
    |  1  |   nrel-pds-building-stock-comstock-athena-2020-comstock-v1-state  | READY |  s3://nrel-pds-building-stock/comstock/athena/2020/comstock_v1/state   | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  2  |            oedi-data-lake-atb-electricity-parquet-2019            | READY |           s3://oedi-data-lake/ATB/electricity/parquet/2019/            | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  3  |            oedi-data-lake-atb-electricity-parquet-2020            | READY |           s3://oedi-data-lake/ATB/electricity/parquet/2020/            | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  4  |            oedi-data-lake-atb-electricity-parquet-2021            | READY |           s3://oedi-data-lake/ATB/electricity/parquet/2021/            | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  5  |                 oedi-data-lake-pv-rooftop-aspects                 | READY |                s3://oedi-data-lake/pv-rooftop/aspects/                 | 2022-01-09 23:28:24+00:00 | 2022-01-09 23:28:24+00:00 |
    |  6  |                oedi-data-lake-pv-rooftop-buildings                | READY |               s3://oedi-data-lake/pv-rooftop/buildings/                | 2022-01-09 23:28:24+00:00 | 2022-01-09 23:28:24+00:00 |
    |  7  |            oedi-data-lake-pv-rooftop-developable-planes           | READY |           s3://oedi-data-lake/pv-rooftop/developable-planes/           | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  8  |          oedi-data-lake-pv-rooftop-pr-developable-planes          | READY |         s3://oedi-data-lake/pv-rooftop-pr/developable-planes/          | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  9  |                   oedi-data-lake-pv-rooftop-rasd                  | READY |                  s3://oedi-data-lake/pv-rooftop/rasd/                  | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  10 |                oedi-data-lake-tracking-the-sun-2018               | READY |               s3://oedi-data-lake/tracking-the-sun/2018/               | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  11 |                oedi-data-lake-tracking-the-sun-2019               | READY |               s3://oedi-data-lake/tracking-the-sun/2019/               | 2022-01-09 23:28:23+00:00 | 2022-01-09 23:28:23+00:00 |
    |  12 |                oedi-data-lake-tracking-the-sun-2020               | READY |               s3://oedi-data-lake/tracking-the-sun/2020/               | 2022-01-09 23:28:24+00:00 | 2022-01-09 23:28:24+00:00 |
    +-----+-------------------------------------------------------------------+-------+------------------------------------------------------------------------+---------------------------+---------------------------+

2. Run Crawler

Use the following command to run one of the glue crawlers that will populate the table in your staging bucket. Be aware that this step will result in a charge to your AWS account, depending on the size of the table. Currently, most of the glue crawlers will cost less than $1 to run, with the exception building-stock which might be more like $10.

.. code-block:: bash

    (oedi) $ oedi aws run-crawler -n oedi-data-lake-tracking-the-sun-2018

If you want it runs in background, use option ``--background-run`` or ``-b``:

.. code-block:: bash

    (oedi) $ oedi aws run-crawler -n oedi-data-lake-tracking-the-sun-2018 -b

3. Run Crawlers

The ``run-crawler`` command can only run one crawler. If you want to run all of crawlers 
at once, you can use ``run-crawlers``, which will start all available crawlers in data lake.

.. code-block:: bash

    (oedi) $ oedi aws run-crawlers

4. List Databases
   
The crawlers populate tables that are contained within databases in your data lake. Run the following code to see a list of available databases.

.. code-block::

    (oedi) $ oedi aws list-databases
    All available databaes are:
    +-----+-----------------------+---------------------+
    | No. |          Name         |      CreateTime     |
    +-----+-----------------------+---------------------+
    |  0  |        default        | 2022-01-13 17:52:42 |
    |  1  |        oedi_atb       | 2022-01-09 23:28:07 |
    |  2  |    oedi_buildstock    | 2022-01-09 23:28:07 |
    |  3  |    oedi_pv_rooftops   | 2022-01-09 23:28:07 |
    |  4  | oedi_tracking_the_sun | 2022-01-09 23:28:06 |
    +-----+-----------------------+---------------------+

5. List Tables

To view a list of tables within a given database, run the following command, specifying the database with the -d option.

.. code-block::

    (oedi) $ oedi aws list-tables -d oedi_tracking_the_sun
    All available tables in [oedi_tracking_the_sun] are:
    +-----+-----------------------+---------------------+
    | No. |          Name         |      CreateTime     |
    +-----+-----------------------+---------------------+
    |  0  | tracking_the_sun_2018 | 2022-01-09 23:48:45 |
    |  1  | tracking_the_sun_2019 | 2022-01-09 23:49:21 |
    |  2  | tracking_the_sun_2020 | 2022-01-09 23:49:23 |
    +-----+-----------------------+---------------------+


Run Queries
-----------
After the desired tables are populated in database, then you can run SQL queries via 
AWS Athena. In this package, we also provide a ``run-query`` command for tests. 
For example:

.. code-block:: bash

    (oedi) $ oedi aws run-query -q "select * from oedi_tracking_the_sun.tracking_the_sun_2020 limit 10"
    data_provider_1 data_provider_2 system_id_1 system_id_2   installation_date  system_size_dc  ...  output_capacity_inverter_3  dc_optimizer inverter_loading_ratio  battery_rated_capacity_kw  battery_rated_capacity_kwh  state
    0  Arkansas State Energy Office           -9999       -9999       -9999 2010-04-29 06:00:00           2.016  ...                       -9999             0               1.178947                    -9999.0                     -9999.0     AR
    1  Arkansas State Energy Office           -9999       -9999       -9999 2010-04-26 06:00:00           3.360  ...                       -9999             0               1.178947                    -9999.0                     -9999.0     AR
    2  Arkansas State Energy Office           -9999       -9999       -9999 2010-04-20 06:00:00          13.440  ...                       -9999             0               1.178947                    -9999.0                     -9999.0     AR
    3  Arkansas State Energy Office           -9999       -9999       -9999 2010-04-21 06:00:00           5.520  ...                       -9999             0               1.210526                    -9999.0                     -9999.0     AR
    4  Arkansas State Energy Office           -9999       -9999       -9999 2010-04-22 06:00:00           2.530  ...                       -9999             0               1.210526                    -9999.0                     -9999.0     AR
    5  Arkansas State Energy Office           -9999       -9999       -9999 2010-04-22 06:00:00           3.450  ...                       -9999             0               1.210526                    -9999.0                     -9999.0     AR
    6  Arkansas State Energy Office           -9999       -9999       -9999 2010-04-21 06:00:00           3.220  ...                       -9999             0               1.210526                    -9999.0                     -9999.0     AR
    7  Arkansas State Energy Office           -9999       -9999       -9999 2010-05-18 06:00:00          12.880  ...                       -9999             0               1.210526                    -9999.0                     -9999.0     AR
    8  Arkansas State Energy Office           -9999       -9999       -9999 2010-06-03 06:00:00           3.360  ...                       -9999             0               1.178947                    -9999.0                     -9999.0     AR
    9  Arkansas State Energy Office           -9999       -9999       -9999 2010-05-11 06:00:00           2.700  ...                       -9999             0           -9999.000000                    -9999.0                     -9999.0     AR

    [10 rows x 78 columns]
    
The query results would be stored in the ``Staging Location`` configured in ``config.yaml``. You can also specify this location 
via ``--output-location`` or ``-o`` in this command, like this:

.. code-block:: bash

    (oedi) $ oedi aws run-query -q "select * from oedi_tracking_the_sun.tracking_the_sun_2020 limit 10" -o "s3://another-output-location/"

Commands Help
-------------

For more ``oedi`` commands information, please use ``--help``.

.. code-block:: bash

    (oedi) $ oedi aws --help
    Usage: oedi aws [OPTIONS] COMMAND [ARGS]...

        OEDI command with AWS cloud.

    Options:
        --help  Show this message and exit.

    Commands:
        list-crawlers   List available crawlers.
        list-databases  List available databases
        list-tables     List available tables in database.
        run-crawler     Run crawler to populate table.
        run-crawlers    Run all crawlers in data lake.
        run-query       Run SQL query and show/export result.

Each command also has its own help page:


.. code-block:: bash

    (oedi) $ oedi aws run-query --help
    Usage: oedi aws run-query [OPTIONS]

        Run SQL query and show/export result.

    Options:
        -q, --query-string TEXT      Valid SQL query string.  [required]
        -s, --staging-location TEXT  A S3 staging directory.
        -r, --region-name TEXT       AWS region name, i.e. us-west-2
        -o, --output-file PATH       Export result to CSV file.
        --head                       Show pandas DataFrame head only.  [default:False]
        --help                       Show this message and exit.
