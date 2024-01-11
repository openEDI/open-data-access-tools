import os
from etl_tools import transform_wtk_h5_file, transform_sup3rcc_h5_file, gen_ref
from time import time
import boto3

# Download h5 to local and then build out the rechunked copy

# Start timer
start_time = time()

# Get input from container environment overrides
container_name = 'oedi'
staging_bucket = os.getenv('staging_bucket')
source_path = os.getenv('s3_file')
file_name = source_path.split('/')[-1]

# Download file to local
s3 = boto3.client('s3')
Bucket = source_path.split('/')[0]
Key = source_path.replace(f'{Bucket}/', '')
local_path = f'/data/{source_path}'
os.makedirs(local_path.replace(file_name, ''))
s3.download_file(Bucket=Bucket, Key=Key, Filename=local_path)

# Transform dataset
print(f'{(time() - start_time) / 60:.2f} min: {file_name} - Starting transformation.')
if 'nrel-pds-wtk' in source_path:
    #DATASET_NAME = 'wtk'
    az_path = source_path.replace('nrel-pds-wtk/', 'wtk/')
    scratch_path = f'/data/{az_path}'
    os.makedirs(scratch_path.replace(file_name, ''), exist_ok=True) # Need to create the dir if it doesn't exist
    transform_wtk_h5_file(local_path, scratch_path, in_file_on_s3=False)
elif 'sup3rcc' in source_path:
    DATASET_NAME = 'sup3rcc'
    az_path = source_path.replace('/nrel-pds-sup3rcc/', 'sup3rcc/')
    scratch_path = f'/data/{az_path}'
    os.makedirs(scratch_path.replace(file_name, ''), exist_ok=True) # Need to create the dir if it doesn't exist
    transform_sup3rcc_h5_file(source_path, scratch_path)
else:
    raise NotImplementedError(f'Dataset for {source_path} not implemented yet.')
print(f'{(time() - start_time) / 60:.2f} min: {file_name} - Transformed.')

ref_file = scratch_path.replace('.h5', '.json')
ref_file_s3 = scratch_path.replace('.h5', '_s3.json')
gen_ref(scratch_path, f'abfs://{container_name}/{az_path}', ref_file=ref_file)
print(f'{(time() - start_time) / 60:.2f} min: {file_name} - Azure reference generated.')

s3_staging_path = f's3://{staging_bucket}/{az_path}'
gen_ref(scratch_path, s3_staging_path, ref_file=ref_file_s3)
print(f'{(time() - start_time) / 60:.2f} min: {file_name} - S3 reference generated.')

# Upload to staging
s3 = boto3.client('s3')
s3.upload_file(ref_file, staging_bucket, ref_file.replace('/data/', ''))
print(f'{(time() - start_time) / 60:.2f} min: {file_name} - Azure reference uploaded to staging.')
s3.upload_file(ref_file_s3, staging_bucket, ref_file_s3.replace('/data/', ''))
print(f'{(time() - start_time) / 60:.2f} min: {file_name} - S3 reference uploaded to staging.')
s3.upload_file(scratch_path, staging_bucket, az_path)
print(f'{(time() - start_time) / 60:.2f} min: {file_name} - h5 file uploaded to staging.')
