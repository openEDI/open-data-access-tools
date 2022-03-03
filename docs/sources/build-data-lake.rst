Build Data Lake
===============

Config OEDI
-----------
First, you need to run ``oedi config sync`` command,

.. code-block:: bash

    (oedi) $ oedi config sync

It'll create a config file ``confi.yaml`` in ``~/.oedi/`` directory,
which contains the default OEDI config settings.

Please edit the ``~/.oedi/confi.yaml`` config file and 
configure the data lake based on your preference. For example, we provide the 
template with following ``AWS`` configurations.

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
    - Identifier: atb_electricity
      Name: oedi_atb_electricity
      Locations:
        - s3://oedi-data-lake/atb/electricity/2019/
        - s3://oedi-data-lake/atb/electricity/2020/
        - s3://oedi-data-lake/atb/electricity/2021/

  Staging Location: s3://user-owned-staging-bucket/

OEDI may have multiple providers in the future. For now, we only focus on ``AWS``.
Those configurations will be applied to AWS and related services in your data lake.

    * ``Regsion Name``: the AWS resources are tied to the Region that you specified.
    * ``Datalake Name``: the stack name of AWS CloudFormation.
    * ``Databases``: the databases that would be created in AWS Glue.
        - ``Identifier``: the string that can identifer the dataset.
        - ``Name``: the name of database that would be created in AWS Glue.
        - ``Locations``: the AWS S3 locations with columnar dataset.
    * ``Staging Location``: the AWS S3 location used by Athena for query outputs.

Please update the values in ``~/.oedi/config.yaml`` based your requirements.

Deploy Data Lake
----------------
After the configurations got determined, then we can use ``cdk`` commands to manage the 
AWS infrustructures required by the data lake.

Please change your directory to ``open-data-access-tools/oedi/AWS`` which contains the CDK
app. 

.. code-block:: bash

    $ cd /open-data-access-tools/oedi/AWS

To deploy the data lake stack configured above, we need to run the command:

.. code-block:: bash

    $ cdk deploy

What happens behind the scene? Based on the configurations provided above, a stack of AWS 
resources are created, updated or deleted. Assume it's the first time that we deploy the 
data lake, then the following resources are created in the data lake.

    * An AWS Glue database was created.
    * An AWS Glue crawler role was assumed, used for creating crawlers.
    * A number of AWS Glue crawlers were created.

Now, you have the data lake infrastructures launched. Later on, after any change to ``config.yaml``,
it's expected to re-deploy via ``cdk`` to apply the updated configurations.

There are also other common ``cdk`` commands, like these:

    * ``cdk ls``, list all stacks in the app.
    * ``cdk synth``, emits the synthesized CloudFormation template.
    * ``cdk diff``, compare deployed stack with current state.
    * ``ckd destroy``, it deletes all AWS resources deployed.
    * ``cdk docs``, open CDK documentation. 

For more information about ``cdk`` commands, please refer to the official documentation -
https://docs.aws.amazon.com/cdk/latest/guide/home.html.
