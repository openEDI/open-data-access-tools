Installation
============
This section covers how to setup an environment used for building your data lake by means of `aws cdk \
<https://docs.aws.amazon.com/cdk/latest/guide/home.html>`_, and how to install ``oedi`` package and
use ``oedi`` commands to run crawlers and test SQL queries.

The easiest way to setup the environment is using ``docker``, but your can also set it up in your local 
environment step by step. Before setting up the environment, please get a copy of the source code from
our public Github repository - `open-data-access-tools <https://github.com/openEDI/open-data-access-tools>`_.

Your can clone using ``git clone``:

.. code-block:: bash

    $ git clone git@github.com:openEDI/open-data-access-tools.git

Please refer to our `releases <https://github.com/openEDI/open-data-access-tools/releases>`_ page, 
and check the datasets included in each release.


Docker Environment
------------------

There are two ways to get the docker image of this tool, either pull from dockerhub, 
or build from the source.

Download Docker Image from DockerHub
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pull the image from DockerHub `repo <https://hub.docker.com/r/openenergydatainitiative/oedi>`_ of OEDI,

.. code-block:: bash

    docker pull openenergydatainitiative/oedi


Build Docker Image from Source Code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Go to the root directory of the source code ``open-data-access-tools``, 
and build docker image using ``docker build`` command,

.. code-block:: bash

    $ cd open-data-access-tools
    $ docker build -t oedi .

After having the image, then run docker container service like this,

.. code-block:: bash

    $ docker run --rm -it -v /home/user/.aws:/root/.aws oedi bash
    $ root@53d985b7f2ba:/open-data-access-tools/#

.. note::

    Option ``-v /home/user/.aws:/root/.aws`` maps the AWS credentials at your local into
    the docker environment, so that ``cdk`` command can run under proper account and permissions.
    Please replace ``/home/user/.aws`` by using the directory that contains your AWS credentials.

Now, you are in ``oedi`` docker environment, and can build and use OEDI data lake.


Local Environment
-----------------

If you want to setup the environment directly into your computer, please follow the steps below.

1. Install `Node.js (>=10.3.0) <https://nodejs.org/en/download/>`_ and `npm <https://www.npmjs.com/>`_ 
to your computer. The ``cdk`` command-line tool and the AWS Construct Library are developed in TypeScript and 
run on `Node.js`, and the bindings for Python use this backend and toolset as well.

2. Create an virutal Python environment for the project.

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

3. Make sure your ``oedi`` virtual environment is activated, then go the root directory of 
``open-data-access-tools`` and install this package editablely.


.. code-block:: bash

    $ workon oedi 
    (oedi) $ cd open-data-access-tools
    (oedi) $ pip install -e .

4. Change work directory to the one that contains AWS CDK app.

.. code-block:: bash

    (oedi) $ cd oedi/AWS
    (oedi) $ pwd
    ~/open-data-access-tools/oedi/AWS

Now, you are in the ``oedi`` local environment, and build and use OEDI data lake.
