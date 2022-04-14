Build Data Lake
===============

Now that you have your ``oedi`` environment set up, you are almost ready to access the data.

Configure OEDI
--------------
First, you need to run the configuration command,

.. code-block:: bash

    (oedi) $ oedi config sync

This will use your ``AWS`` credentials file to establish access to the cloud. It will also create a config file, 
``config.yaml``, in ``~/.oedi`` directory, which contains the default OEDI config settings.

You need to edit this file to select the data you would like to access and provide the path to your s3 staging location. Here is what the default config file will look like:

.. code-block:: bash

  (oedi) $ oedi config show --provider AWS
  AWS:
    Region Name: us-west-2
    Datalake Name: oedi-data-lake
    Databases:
      - Identifier: pv_rooftops
        Name: oedi_pv_rooftops
        Locations:
          - s3://oedi-data-lake/pv-rooftop/aspects/
          - s3://oedi-data-lake/pv-rooftop/buildings/
          - s3://oedi-data-lake/pv-rooftop/developable-planes/
          - s3://oedi-data-lake/pv-rooftop/rasd/
          - s3://oedi-data-lake/pv-rooftop-pr/developable-planes/
      - Identifier: buildstock
        Name: oedi_buildstock
        Locations:
          - s3://nrel-pds-building-stock/comstock/athena/2020/comstock_v1/state
          - s3://nrel-pds-building-stock/comstock/athena/2020/comstock_v1/metadata
      - Identifier: tracking_the_sun
        Name: oedi_tracking_the_sun
        Locations:
          - s3://oedi-data-lake/tracking-the-sun/2018/
          - s3://oedi-data-lake/tracking-the-sun/2019/
          - s3://oedi-data-lake/tracking-the-sun/2020/
      - Identifier: atb
        Name: oedi_atb
        Locations:
          - s3://oedi-data-lake/ATB/electricity/parquet/2019/
          - s3://oedi-data-lake/ATB/electricity/parquet/2020/
          - s3://oedi-data-lake/ATB/electricity/parquet/2021/
      - Identifier: pvdaq
        Name: oedi_pvdaq
        Locations:
        - s3://oedi-data-lake/pvdaq/parquet/site/
        - s3://oedi-data-lake/pvdaq/parquet/system/
        - s3://oedi-data-lake/pvdaq/parquet/inverters/
        - s3://oedi-data-lake/pvdaq/parquet/meters/
        - s3://oedi-data-lake/pvdaq/parquet/metrics/
        - s3://oedi-data-lake/pvdaq/parquet/modules/
        - s3://oedi-data-lake/pvdaq/parquet/mount/
        - s3://oedi-data-lake/pvdaq/parquet/other-instruments/
        - s3://oedi-data-lake/pvdaq/parquet/pvdata/
    Staging Location: s3://user-owned-staging-bucket/

OEDI may have multiple providers in the future. For now, we only focus on ``AWS``.
Those configurations will be applied to AWS and related services in your data lake.

    * ``Region Name``: the AWS resources are tied to the Region that you specified.
    * ``Datalake Name``: the stack name of AWS CloudFormation.
    * ``Databases``: the databases that will be created in AWS Glue.
        - ``Identifier``: the string that can identifer the dataset.
        - ``Name``: the name of database that would be created in AWS Glue.
        - ``Locations``: the AWS S3 locations with columnar dataset.
    * ``Staging Location``: the AWS S3 location used by Athena for query outputs.

Use a text editor of your choice to modify the file (e.g. ``vi oedi/config.yaml``). At a minimum, you will need to change the staging location to a bucket that your ``AWS`` account has access to. If you provide a path to a bucket that does not exist, then AWS will create the bucket for you. However, bucket names must be unique across the whole region, so you cannot use a bucket name that someone else has taken. You can either use a bucket name that you think is unlikely to exist, or use the ``AWS`` managment console to create the bucket in your account manually, so that you know it will work (don't forget to choose the correct region!).

Additionally, you should only choose the databases and tables that you are interested in so that your ``AWS`` costs are minimized.

Deploy Data Lake
----------------
Once your configuration file is ready, you can use ``cdk`` commands to manage the 
AWS infrastructure required by the data lake.

Please change your directory to ``open-data-access-tools/oedi/AWS`` which contains the CDK
app. 

.. code-block:: bash

   (oedi) $ cd /open-data-access-tools/oedi/AWS

To deploy the data lake stack configured above, we need to run the command:

.. code-block:: bash

   (oedi) $ cdk deploy

What happens behind the scene? Based on the configurations provided above, a stack of AWS 
resources are created, updated or deleted. Assume it's the first time that we deploy the 
data lake, then the following resources are created in the data lake.

    * An AWS Glue database was created.
    * An AWS Glue crawler role was assumed, used for creating crawlers.
    * A number of AWS Glue crawlers were created.

Now, you have the data lake infrastructure launched. Later on, after any change to ``config.yaml``, you will need to re-deploy via ``cdk deploy`` to apply the updated configurations.

There are also other common ``cdk`` commands, like these:

    * ``cdk ls``, list all stacks in the app.
    * ``cdk synth``, emits the synthesized CloudFormation template.
    * ``cdk diff``, compare deployed stack with current state.
    * ``cdk destroy``, it deletes all AWS resources deployed.
    * ``cdk docs``, open CDK documentation. 

For more information about ``cdk`` commands, please refer to the official documentation -
https://docs.aws.amazon.com/cdk/latest/guide/home.html.
