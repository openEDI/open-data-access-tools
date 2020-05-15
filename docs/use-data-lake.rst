Use Data Lake
=============

Populate Tables
---------------
The step above created a database ``oedi_database`` in AWS Glue, but it's empty. Now, we need 
to run the crawlers to populate the tables in the database. 

Make sure your ``oedi`` virtual environment created before has already been activated. Then,
use the commands below to check and run the crawler.

1. List Crawlers

.. code-block:: bash

    (oedi) $ oedi list-crawlers
    - lbnl-tracking-the-sun-2018
    - lbnl-tracking-the-sun-2019
    - nrel-pv-rooftops-aspects
    - nrel-pv-rooftops-buildings
    - nrel-pv-rooftops-developable-planes
    - nrel-pv-rooftops-rasd

2. Run Crawler

.. code-block:: bash

    (oedi) $ oedi run-crawler -n lbnl-tracking-the-sun-2018

If you want it runs in background, use option ``--background-run`` or ``-b``:

.. code-block:: bash

    (oedi) $ oedi run-crawler -n lbnl-tracking-the-sun-2018 -b

3. Run Crawlers

The command ``run-crawler`` can only run one crawler, if you want to run all of crawlers 
at one time, please use ``run-crawlers``, it would start all available crawlers in data lake.

.. code-block:: bash

    (oedi) $ oedi run-crawlers


4. List Tables

The crawlers populated tables in the database after runs, please use the command 
below to list the tables in it.

.. code-block:: bash

    (oedi) $ oedi list-tables
    - oedi_database_test.lbnl_tracking_the_sun_2018
    - oedi_database_test.lbnl_tracking_the_sun_2019
    - oedi_database_test.nrel_pv_rooftops_aspects
    - oedi_database_test.nrel_pv_rooftops_buildings
    - oedi_database_test.nrel_pv_rooftops_developable_planes
    - oedi_database_test.nrel_pv_rooftops_rasd


Run Queries
-----------
After the desired tables are populated in database, then you can run SQL queries via 
AWS Athena. In this package, we also provide ``run-query`` command for tests. 
For example,

.. code-block:: bash

    (oedi) $ oedi run-query -q "select * from oedi_database_test.lbnl_tracking_the_sun_2018 limit 10"
                                       data_provider system_id_from_data_provider system_id_tracking_the_sun installation_date  system_size  ...  microinverter_1  microinverter_2  microinverter_3  dc_optimizer  state
    0  Department of Commerce & Economic Opportunity                        -9999                  IL_DCEO_1        2010-08-06         3.00  ...            -9999            -9999            -9999         -9999     IL
    1  Department of Commerce & Economic Opportunity                        -9999                  IL_DCEO_2        2010-08-05         4.10  ...            -9999            -9999            -9999         -9999     IL
    2  Department of Commerce & Economic Opportunity                        -9999                  IL_DCEO_3        2008-07-09         3.10  ...            -9999            -9999            -9999         -9999     IL
    3  Department of Commerce & Economic Opportunity                        -9999                  IL_DCEO_4        2008-08-04         4.80  ...            -9999            -9999            -9999         -9999     IL
    4  Department of Commerce & Economic Opportunity                        -9999                  IL_DCEO_7        2003-11-30        18.00  ...            -9999            -9999            -9999         -9999     IL
    5  Department of Commerce & Economic Opportunity                        -9999                  IL_DCEO_8        2010-08-17         4.00  ...            -9999            -9999            -9999         -9999     IL
    6  Department of Commerce & Economic Opportunity                        -9999                  IL_DCEO_9        2011-04-22        24.00  ...            -9999            -9999            -9999         -9999     IL
    7  Department of Commerce & Economic Opportunity                        -9999                 IL_DCEO_10        2010-01-31        54.70  ...            -9999            -9999            -9999         -9999     IL
    8  Department of Commerce & Economic Opportunity                        -9999                 IL_DCEO_11        2003-11-30        32.40  ...            -9999            -9999            -9999         -9999     IL
    9  Department of Commerce & Economic Opportunity                        -9999                 IL_DCEO_12        2008-12-12         4.32  ...            -9999            -9999            -9999         -9999     IL

    [10 rows x 63 columns]

The query results would be stored in ``Staging Location`` configured in ``config.yaml``. You can also specify this location 
via ``--output-location`` or ``-o`` in this command, like this:

.. code-block:: bash

    (oedi) $ oedi run-query -q "select * from oedi_database_test.lbnl_tracking_the_sun_2018 limit 10" -o "s3://another-outpu-location/"


Commands Help
-------------

For more ``oedi`` commands information, please use ``--help``.

.. code-block:: bash

    (oedi) $ oedi --help
    Usage: oedi [OPTIONS] COMMAND [ARGS]...

    Options:
    --help  Show this message and exit.

    Commands:
    list-crawlers  List available crawlers.
    list-tables    List available tables.
    run-crawler    Run crawler to populate table.
    run-crawlers   Run all crawlers in data lake.
    run-query      Run SQL query and show result.
    show-config    Show data lake configuration.

For how to use the command above, try like this,


.. code-block:: bash

    (oedi) $ oedi run-query --help
    Usage: oedi run-query [OPTIONS]

    Run SQL query and show result.

    Options:
    -q, --query-string TEXT     Valid SQL query string.  [required]
    -o, --output-location TEXT  A S3 staging directory.
    -r, --region-name TEXT      AWS region name, i.e. us-west-2
    --head                      Show pandas DataFrame head only.  [default:
                                False]
    --help                      Show this message and exit.
