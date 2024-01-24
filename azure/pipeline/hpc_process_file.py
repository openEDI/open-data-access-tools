import sys
import os
from etl_tools import transform_wtk_h5_file, transform_sup3rcc_h5_file, gen_ref
from hpc_tools import construct_paths
from time import time
import logging

# Start timer
start_time = time()

CONTAINER_NAME = 'oedi'
USER = os.getenv('USER')

# Get input
args = sys.argv
if len(args) != 2:
    raise Exception('Must provide exactly one file path.')
source_path = args[1]

# Construct paths
file_name, job_name, job_dir, ref_file, az_path = construct_paths(source_path)
scratch_path = f'{job_dir}{file_name}'
if 'WIND' in source_path:
    transform_wtk_h5_file(source_path, scratch_path)
elif 'sup3rcc' in source_path:
    transform_sup3rcc_h5_file(source_path, scratch_path)
else:
    raise NotImplementedError(f'The only Eagle datasets that have been implemented are WIND and sup3rcc.')
logging.info(f'{(time() - start_time) / 60:.2f} min: {file_name} transformed.')

# Generate references
gen_ref(scratch_path, f'abfs://{CONTAINER_NAME}/{az_path}', ref_file=ref_file)
logging.info(f'{(time() - start_time) / 60:.2f} min: {job_name} references generated.')
