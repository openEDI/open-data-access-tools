import planetary_computer
import sys
import subprocess
from etl_tools import load_oedi_sas
import os

args = sys.argv

blob_address = 'https://nrel.blob.core.windows.net/oedi'
sas_token = load_oedi_sas()

for arg in args[1:]:
    source, dest = arg.split(':')
    source = f"'{source}'"
    dest = f"'{blob_address}/{dest}?{sas_token}'"

    os.system(f'azcopy copy {source} {dest} --overwrite ifSourceNewer')
    #subprocess.run(['azcopy', 'copy', source, dest])
