import sys
import os
from etl_tools import transform_wtk_h5_file, transform_sup3rcc_h5_file, gen_ref
from hpc_tools import construct_paths
from time import time

# Start timer
start_time = time()

CONTAINER_NAME = 'oedi'
USER = os.getenv('USER')

# Get input
args = sys.argv
if len(args) != 2:
    raise Exception('Must provide exactly one file path.')
source_path = args[1]
# if 'WIND' in source_path:
#     DATASET_NAME = 'wtk'
#     file_path = source_path.replace('/datasets/WIND/', 'wtk/')
# elif 'sup3rcc' in source_path:
#     DATASET_NAME = 'sup3rcc'
#     file_path = source_path.replace('/datasets/', '')
# else:
#     raise NotImplementedError(f'Dataset for {source_path} not implemented yet.')

# Copy h5 file to scratch and transform
#scratch_path = f'/scratch/{USER}/{file_path}'

# Construct paths
file_name, job_name, job_dir, ref_file, az_path = construct_paths(source_path)
scratch_path = f'{job_dir}{file_name}'
#scratch_path = source_path.replace('/datasets', f'/scratch/{USER}')
#file_name = scratch_path.split('/')[-1]
# scratch_dir = scratch_path.replace(file_name, '')
# os.makedirs(scratch_dir, exist_ok=True)
# if DATASET_NAME == 'wtk':
if 'WIND' in source_path:
    # shutil.copy(source_path, scratch_path)
    # print(f'{(time() - start_time) / 60:.2f} min: {file_name} copied.')
    transform_wtk_h5_file(source_path, scratch_path)
elif 'sup3rcc' in source_path:
    transform_sup3rcc_h5_file(source_path, scratch_path)
else:
    raise NotImplementedError(f'The only Eagle datasets that have been implemented are WIND and sup3rcc.')

print(f'{(time() - start_time) / 60:.2f} min: {file_name} transformed.')

# with open(f'/home/{USER}/Azure_workflow/temp_files_{DATASET_NAME}.txt', 'a') as f:
#     f.write(f'{scratch_path}\n')

# Generate a kerchunk reference file for the h5 file
# ref_file = scratch_path.replace('.h5', '_ref.json')
# file_name = ref_file.split('/')[-1]
#ref_dir = ref_file.replace(file_name, '')
#os.makedirs(ref_dir, exist_ok=True) # Need to create the dir if it doesn't exist
gen_ref(scratch_path, f'abfs://{CONTAINER_NAME}/{az_path}', ref_file=ref_file)
print(f'{(time() - start_time) / 60:.2f} min: {job_name} references generated.')
