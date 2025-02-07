#!/bin/bash --login
set -e

# activate conda environment and let the following process take over
conda activate oedi-azure-container
exec "$@"
