## Azure CLI Guide

OEDI data exist as blobs in Azure. Blobs live in containers. Containers live in storage accounts. For most of our data, the storage account is 'nrel' and the container is 'oedi'. There is a directory structure within the container to organize different data sets. Currently, the datasets present are 'PR100', 'pv-rooftop' and part of 'sup3rcc'. NSRDB lives in the 'nrel' storage account but in a different container called 'nrel-nsrdb'.

In order to access data from the command line, you will need to obtain a temporary SAS token from the planetary computer. You can then use that token as an argument for any commands you make with the CLI. CLI reference for interacting with blobs: https://learn.microsoft.com/en-us/cli/azure/storage/blob?view=azure-cli-latest#az-storage-blob-download

Finally, if the goal is to move large amounts of data from blob storage to S3 or local, then the best tool is azcopy: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10

Obtain a planetary computer temporary access token:

`curl https://planetarycomputer.microsoft.com/api/sas/v1/token/nrel/oedi > sas.json`

View a list of blobs in the PR100 dataset:

`az storage blob list --account-name nrel --container-name oedi --output table --prefix PR100 --sas-token "<SAS Token>"`

Download a blob from the PR100 dataset:

`az storage blob download --account-name nrel --container-name oedi --name PR100/Infrastructure/setbacks_runway.parquet --file setbacks_runway.parquet --sas-token "<SAS Token>"`
