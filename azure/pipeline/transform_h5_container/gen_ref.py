import ujson
from etl_tools import gen_ref_comb, load_oedi_sas
from aws_tools import copy_local_file_to_azure
import xarray as xr
import os
import s3fs
import h5py

# TODO: Remove all az stuff. The az combined ref file gets created at a later step, after data moves to Azure

# Azure container name
CONTAINER_NAME = 'oedi'

# Access S3
s3 = s3fs.S3FileSystem()

# Get input from container environment
# s3_source_files = ujson.loads(os.getenv('s3_files'))
s3_comb_ref_file = os.getenv('s3_comb_ref_file')
# az_comb_ref_file = os.getenv('az_comb_ref_file')
staging_bucket = os.getenv('staging_bucket')
run_name = os.getenv('run_name')

# Get s3 file list from input file on s3 (This list was too long to be an env variable.)
with s3.open(f'{staging_bucket}/{run_name}.json') as f:
    input_data = ujson.load(f)
s3_source_files = input_data['s3_files']

# Get paths to references and list of identical dims
test_file = s3_source_files[0]
if 'nrel-pds-wtk' in test_file:
    s3_ref_paths = [f"{staging_bucket}/{f.replace('nrel-pds-wtk', 'wtk').replace('.h5', '_s3.json')}" for f in s3_source_files]
    az_ref_paths = [f"{staging_bucket}/{f.replace('nrel-pds-wtk', 'wtk').replace('.h5', '.json')}" for f in s3_source_files]
    test_file = test_file.replace('nrel-pds-wtk', 'wtk')
    with s3.open(f'{staging_bucket}/{test_file}') as f:
        h5 = h5py.File(f)
        identical_dims = list(h5.attrs['identical_dims'])
elif 'sup3rcc' in test_file:
    identical_dims = ['country', 'county', 'eez', 'elevation', 'latitude', 'longitude', 'offshore', 'state', 'timezone']
    raise NotImplementedError()
else:
    NotImplementedError()

# Open all reference files
s3_refs = []
az_refs = []
for s3_rp, az_rp in zip(s3_ref_paths, az_ref_paths):
    with s3.open(s3_rp, 'rb') as f:
        s3_refs.append(ujson.load(f))
    with s3.open(az_rp, 'rb') as f:
        az_refs.append(ujson.load(f))

# Generate the combined reference files
if s3_comb_ref_file:
    local_s3_ref = 's3_ref.json'
    gen_ref_comb(s3_refs, ref_file=local_s3_ref, identical_dims=identical_dims, remote_protocol='s3')
    s3.put_file(local_s3_ref, f's3://{staging_bucket}/{s3_comb_ref_file}')

# if az_comb_ref_file:
#     local_az_ref = 'az_ref.json'
#     gen_ref_comb(az_refs, ref_file=local_az_ref, identical_dims=identical_dims, remote_protocol='abfs')
#     s3.put_file(local_az_ref, f's3://{staging_bucket}/{az_comb_ref_file}')
#     copy_local_file_to_azure(local_az_ref, az_comb_ref_file)
