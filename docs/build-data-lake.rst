Build Data Lake
===============

Config OEDI
-----------
Please go to the root directory of ``open-data-access-tools`` and 
configure the data lake based on your needs. We provide a template
``config.yaml`` with following configurations.

.. code-block:: bash

    (oedi) $ oedi show-config
    PROVIDER-AWS:
      AWS Region: us-west-2
      Data Lake Name: oedi-data-lake
      Database Name: oedi_database
      Dataset Locations:
      - s3://lbnl-tracking-the-sun/2018/
      - s3://lbnl-tracking-the-sun/2019/
      - s3://nrel-pv-rooftops/aspects/
      - s3://nrel-pv-rooftops/buildings/
      - s3://nrel-pv-rooftops/developable_planes/
      - s3://nrel-pv-rooftops/rasd/
      Staging Location: s3://your-output-bucket/folder/

OEDI may have multiple providers in the future. For now, we only focus on ``PROVIDER-AWS``.
Those configurations will be applied to AWS and related services in your data lake.

    * ``AWS Regsion``: the AWS resources are tied to the Region that you specified.
    * ``Data Lake Name``: the stack name of AWS CloudFormation.
    * ``Database Name``: the database name created in AWS Glue.
    * ``Dataset Locations``: the AWS S3 locations with columnar dataset.
    * ``Staging Location`` (optional): the AWS S3 location used by Athena for query outputs.

Please update the values in ``config.yaml`` based your requirements.

Deploy Data Lake
----------------
After the configurations got determined, then we can use ``cdk`` commands to manage the 
AWS infrustructures required by the data lake.

Please change your directory to ``open-data-access-tools/oedi/AWS`` which contains the CDK
app. 

.. code-block:: bash

    $ cd open-data-access-tools/oedi/AWS

To deploy the data lake stack configured above, we need to run the command:

.. code-block:: bash

    $ cdk deploy

What happens behind the scene? Based on the configurations provided above, a stack of AWS 
resources are created, updated or deleted. Assume it's the first time that we deploy the 
data lake, then the following resources are created in the data lake.

    * An AWS Glue database was created.
    * An AWS Glue crawler role was assumed, used for creating crawlers.
    * A number of AWS Glue crawlers were created.

Now, you have the data lake infrustructures lauched. Later on, after any change to ``config.yaml``,
it's exptected to re-deploy via ``cdk`` to apply the updated configurations.

There are also other common ``cdk`` commands, like these:

    * ``cdk ls``, list all stacks in the app.
    * ``cdk synth``, emits the synthesized CloudFormation template.
    * ``cdk diff``, compare deployed stack with current state.
    * ``ckd destroy``, it deletes all AWS resources deployed.
    * ``cdk docs``, open CDK documentation. 

For more information about ``cdk`` commands, please refer to the official documentation -
https://docs.aws.amazon.com/cdk/latest/guide/home.html.
