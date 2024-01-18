import ujson
from etl_tools import gen_ref_comb, load_oedi_sas
import xarray as xr
import planetary_computer
import os
import sys
import logging

# Get input
# First arg should be the path for the combined ref file
# Next should be any number of paths to individual ref files

args = sys.argv
comb_ref_file = args[1]
ref_paths = args[2:]

USER = os.getenv('USER')
CONTAINER_NAME = 'oedi'

az_path = comb_ref_file.replace(f'/scratch/{USER}/', '')

if 'sup3rcc' in ref_paths[0]:
    DATASET = 'sup3rcc'
    identical_dims = ['country', 'county', 'eez', 'elevation', 'latitude', 'longitude', 'offshore', 'state', 'timezone']
elif 'WIND' in ref_paths[0]:
    DATASET = 'wtk'
    az_path = az_path.replace('WIND/', 'wtk/')
    # Open one dataset to get the identical_dims attribute
    token = planetary_computer.sas.get_token('nrel', CONTAINER_NAME).token
    ds = xr.open_dataset(
        "reference://", engine="zarr",
        backend_kwargs={
            "storage_options": {
                "fo": ref_paths[0],
                "remote_protocol": "abfs",
                "remote_options": {'account_name': 'nrel', "credential": token}
            },
            "consolidated": False,
        }
    )
    identical_dims = ds.attrs['identical_dims']
else:
    raise NotImplementedError('The only implemented Eagle datasets are sup3rcc and WIND.')

logging.info(f'Identical dims: {identical_dims}')

# Open all reference files
refs = []
for rp in ref_paths:
    with open(rp, 'rb') as f:
        refs.append(ujson.load(f))

# Generate the combined reference file
gen_ref_comb(refs, ref_file=comb_ref_file, identical_dims=identical_dims)

# Send to Azure
sas_token = load_oedi_sas()
blob_address = f'https://nrel.blob.core.windows.net/{CONTAINER_NAME}'
dest = f'{blob_address}/{az_path}?{sas_token}'
os.system(f'azcopy cp "{comb_ref_file}" "{dest}"')
