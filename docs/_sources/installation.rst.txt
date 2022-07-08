Installation
============
This section covers how to setup an environment used for building your data lake by means of the `aws cdk \
<https://docs.aws.amazon.com/cdk/latest/guide/home.html>`_, and how to install the ``oedi`` package and use ``oedi`` commands to run crawlers and test SQL queries.

The easiest way to setup the environment is using Docker, but you can also set it up in your local
environment step by step.

Please refer to the `oedi S3 viewer <https://data.openei.org/s3_viewer?bucket=oedi-data-lake>`_ 
for information about what data sets are currently available.

Docker Environment
------------------

First, you will need to install and configure Docker. To do this, please refer to `Docker's documentation <https://docs.Docker.com/get-docker/>`_ for your specific machine. Once you have Docker installed, there are two ways to obtain the Docker image of the ``oedi`` tools: either pull it from Docker Hub,
or build it from the source code.

Pull Docker Image from Docker Hub
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The simplest way to obtain the Docker image is to pull it directly from our Docker Hub `repo <https://hub.Docker.com/r/openenergydatainitiative/oedi>`_. To do anything with Docker, you will first need to get an instance of the Docker daemon running. If you installed Docker Desktop, then you just need to open the app, and the daemon will start automatically. Next, open a command line and run:

.. code-block:: bash

    $ docker pull openenergydatainitiative/oedi

If you are using Docker Desktop, you should now see the image under the Images tab. Alternatively, you can run ``docker images`` in the terminal to see a list of images.


.. note::

    The deployment package of AWS data lake was migrated from ``cdk1`` to ``cdk2``. The last version that supports ``cdk1`` is `v0.1.6 <https://github.com/openEDI/open-data-access-tools/releases/tag/v0.1.6>`_ .
    From ``v0.2.0``, the AWS data lake deployment starts to use ``cdk2``. As ``cdk2`` does not include the experimental L2/L3 constructs which were used by this package before ``v0.1.6`` (included),
    it caused compatibility issue related to Glue databases. If you already deployed the data lake, please destroy before trying to re-deploy with new versions.

Build Docker Image from Source Code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're having trouble with Docker Hub, you can have Docker build the image from a clone of our repo. Get a copy of the source code from our public Github repository - `open-data-access-tools <https://github.com/openEDI/open-data-access-tools>`_:

.. code-block:: bash

    $ git clone git@github.com:openEDI/open-data-access-tools.git

In the terminal, navigate to the directory where you saved the source code, ``open-data-access-tools``,
and build the Docker image using the ``build`` command:

.. code-block:: bash

    $ cd <path to open-data-access-tools folder>
    $ docker build -t openenergydatainitiative/oedi .

If you are using Docker Desktop, you should now see the image under the Images tab. Alternatively, you can run ``docker images`` in the terminal to see a list of images.

Run OEDI Docker Container
^^^^^^^^^^^^^^^^^^^^^^^^^

In order to use this tools, you'll need to have an AWS account and provide your  `AWS credentials <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html>`_.

The AWS credentials could be specified with the ``docker run`` command, there are many potential ways, here we provide three, you can use any of them.

1. Attach the ``.aws`` with ``--volume / -v`` flag

.. code-block:: bash

    $ docker run --rm -it \
    -v <path to credentials>:/root/.aws \
    openenergydatainitiative/oedi bash

2. Pass AWS environment variables with ``--env / -e`` flag

.. code-block:: bash

    $ docker run --rm -it \
    -e AWS_ACCESS_KEY_ID=<YOUR KEY ID> \
    -e AWS_SECRET_ACCESS_KEY=<YOUR SECRET EKY> \
    -e AWS_DEFAULT_REGION=<AWS REGION> \
    openenergydatainitiative/oedi bash

3. Pass AWS environment variables with ``--env-file`` flag

  Create a text file, for example, named ``credentials.txt```, and save AWS credentials information,

.. code-block:: bash

    AWS_ACCESS_KEY_ID=<YOUR KEY ID>
    AWS_SECRET_ACCESS_KEY=<YOUR SECRET EKY>
    AWS_DEFAULT_REGION=<AWS REGION>

Then run the docker container like this,

.. code-block:: bash

    $ docker run --rm -it \
    --env-file credentials.txt \
    openenergydatainitiative/oedi bash

Now, you are in an ``oedi`` container environment, and then can build and use your OEDI data lake!


Local Environment
-----------------

If you want to setup the environment directly into your computer, please follow the steps below.

1. Get a copy of the source code from our public Github repository - `open-data-access-tools <https://github.com/openEDI/open-data-access-tools>`_:

.. code-block:: bash

    $ git clone git@github.com:openEDI/open-data-access-tools.git

2. Install `Node.js (>=10.3.0) <https://nodejs.org/en/download/>`_ and `npm <https://www.npmjs.com/>`_ 
to your computer. The ``cdk`` command-line tool and the AWS Construct Library are developed in TypeScript and 
run on `Node.js`, and the bindings for Python use this backend and toolset as well.

3. Create a virutal Python environment for the project.

It's recommended to create a virtual environment for a Python project. There are many tools and 
tutorials online about this, like `virtualenv <https://virtualenv.pypa.io/en/latest/>`_, 
``virtualenv`` with `virtualenvwrapper <https://virtualenvwrapper.readthedocs.io/en/latest/>`_, 
`pipenv <https://github.com/pypa/pipenv>`_, `conda <https://docs.conda.io/en/latest/>`_, etc. 
You can choose based on your own perference. Here, we use ``virtualenv`` with ``virtualenvwrapper`` as 
an example.

.. code-block::

    # Make virtual environment
    $ mkvirtualenv -p python3 oedi

    # Activate virtual environment
    $ workon oedi

    # Deactivate virtual environment
    (oedi) $ deactivate

4. Make sure your ``oedi`` virtual environment is activated, then go the root directory of 
``open-data-access-tools`` and install this package editablely.

.. code-block:: bash

    $ workon oedi 
    (oedi) $ cd open-data-access-tools
    (oedi) $ pip install -e .

5. Change work directory to the one that contains AWS CDK app.

.. code-block:: bash

    (oedi) $ cd oedi/AWS
    (oedi) $ pwd
    ~/open-data-access-tools/oedi/AWS

Now, you are in the ``oedi`` local environment, and build and use OEDI data lake.
