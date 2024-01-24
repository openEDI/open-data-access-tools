import h5py
import pandas as pd
import numpy as np
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
import ujson
import subprocess
import planetary_computer
import os
from time import time
import s3fs
import logging

def time_index_bytestring_to_float(dset):
    t = pd.Series(dset)
    t = t.str.decode('utf8')
    t = t.str.split('+', expand=True)[0]
    t = np.array(t,dtype=np.datetime64)
    t = t.astype('int')
    return t

def copy_attrs(obj1, obj2):
    # Copy the attributes from obj1 to obj2, which may be h5 File objects or h5 dataset objects
    for key in obj1.attrs.keys():
        obj2.attrs[key] = obj1.attrs[key]

def copy_dataset(f_in, f_out, var, mem_limit_GB=80):
    # Determine sizes of slices to read
    dtype_size = f_in[var].dtype.itemsize
    max_read_size = mem_limit_GB * 10 ** 9 # Read 80 GB at a time
    time_index_read_size = f_in[var].shape[0]  # Read all time values
    gid_index_read_size = int(max_read_size / time_index_read_size // dtype_size) # Number of sites to read at a time

    # Create slices
    end = f_in[var].shape[1]
    starts = np.arange(0, end, gid_index_read_size)
    stops = starts[1:]
    stops = np.append(stops, end)
    
    # Copy slices
    for start, stop in zip(starts, stops):
        f_out[var][:, start:stop] = f_in[var][:, start:stop]

def elapsed_time(st):
    return f'{(time() - st) / 60:.2f} min'

def load_oedi_sas():
    # read/write sas token must be stored in a plain text file located at $HOME/.sas or oedi_azure/.sas
    home = os.path.expanduser('~')
    if os.path.isfile(f'{home}/.sas'):
        path = f'{home}/.sas'
    elif os.path.isfile('./.sas'):
        path = './.sas'
    elif os.path.isfile('../.sas'):
        path = '../.sas'
    else:
        path = None
    
    if path:
        with open(path) as f:
            sas = f.read()
    else:
        raise Exception('.sas file not found. Please save your read/write .sas token to a file called .sas located in the oedi_azure directory.')

    return sas

def transform_wtk_h5_file(in_file, out_file, chunk_size=2, weeks_per_chunk=None, in_file_on_s3=False):
    # This is an updated version of transform_h5_file, designed for wtk. wtk does not have a nice rectangular coordinate grid,
    # so the data will be left in 2 dims rather than be converted to 3 dims.

    # h5_file should be a path to a local h5 file. The file will be opened in write-mode, transformed and then closed.
    # chunk_size is the desired size of each chunk in MiB
    # weeks_per_chunk determines the length of chunks in the time_index dimension

    # Summary of data transformations:

    # 1. time_index is converted from byte-string to int (when read by xarray, this will automatically convert to np.datetime64)
    # 2. A gid dataset is created to index the locations
    # 3. time_index and gid are converted to dimension scales
    # 4. Each variable is rechunked so that we will have consistent chunk sizes accross all files
    # 5. The dimension scales are attached to each variable's dimensions
    # 6. The scale_factor metadata is inverted (new_sf = 1 / old_sf)
    # 7. The meta variable is unpacked

    # Notes:
    # Once again, the download/upload steps are what will take all of the time here. To scale up to wtk, this transformation
    # should either happen on Eagle (where the data are already local) or the transformation should be containerized for use
    # with AWS batch.

    # Begin logging
    st = time()
    file_name = out_file.split('/')[-1]
    logging.info(f'{elapsed_time(st)} - {file_name}: Starting transformation.')

    # Open input file
    if in_file_on_s3:
        s3 = s3fs.S3FileSystem()
        f_in = h5py.File(s3.open(in_file))
    else:
        f_in = h5py.File(in_file, 'r')
    
    # Delete output file if it exists, and then create it (note that 'w' mode for h5py would be better, but is unreliable)
    if os.path.exists(out_file):
        os.remove(out_file)
    f_out = h5py.File(out_file, 'a')

    # Copy file attrs
    copy_attrs(f_in, f_out)
    logging.info(f'{elapsed_time(st)} - {file_name}: File attrs copied!')

    # Get the length of time_index and coordinates
    time_len = f_in['time_index'].len()
    nloc = len(f_in['coordinates'])

    # Convert time_index from  bytes to float.
    t = time_index_bytestring_to_float(f_in['time_index'])

    # Create time_index variable in new file. 'units' metadata required for xarray to interpret as datetime.
    f_out.create_dataset('time_index', data=t)
    copy_attrs(f_in['time_index'], f_out['time_index'])
    f_out['time_index'].attrs['units'] = b'seconds since 1970-01-01'

    # Create gid variable
    f_out.create_dataset('gid', data=np.arange(nloc, dtype=np.int32), fillvalue=-1)
    logging.info(f'{elapsed_time(st)} - {file_name}: gid created.')

    # Convert to dimension scales
    f_out['time_index'].make_scale()
    f_out['gid'].make_scale()

    # Determine time_index chunksize
    time_step = t[1] - t[0]
    if not weeks_per_chunk:
        if time_step == 5 * 60:     # 5min data
            weeks_per_chunk = 1
        elif time_step == 10 * 60:  # 10min data
            weeks_per_chunk = 2
        elif time_step == 15 * 60:  # 15min data
            weeks_per_chunk = 3
        elif time_step == 60 * 60:  # hourly data
            weeks_per_chunk = 12
        else:
            weeks_per_chunk = 8     # other resolution
            logging.info(f'Warning: Non-standard resolution of {time_step / 60} min detected.')

    time_index_chunk_len = int(min(weeks_per_chunk * 7 * 24 * 60 * 60 / time_step, time_len))

    logging.info(f'{elapsed_time(st)} - {file_name}: time_index and gid created')

    # Get var names
    vars = [var for var in f_in.keys() if var not in ['meta', 'time_index', 'latitude', 'longitude', 'gid', 'coordinates']]

    # Loop over vars copying them to the new file
    for var in vars:
        logging.info(f'{elapsed_time(st)} - {file_name}: Processing {var}...')
        
        # Check dims
        if not f_in[var].shape[0] == time_len:
            raise Exception(f'Dim 0 of {var} has different length than time_index.')
        if not f_in[var].shape[1] == nloc:
            raise Exception(f'Dim 1 of {var} has different length than gid.')

        # Determine location chunk size
        element_size = f_in[var].dtype.itemsize    # size of single element in bytes
        gid_chunk_len = int(min(chunk_size * 2 ** 20 / time_index_chunk_len // element_size, nloc))

        # Create dataset in new file
        chunks=(time_index_chunk_len, gid_chunk_len)
        f_out.create_dataset(var, shape=f_in[var].shape, dtype=f_in[var].dtype, chunks=chunks)
        copy_dataset(f_in, f_out, var)
        copy_attrs(f_in[var], f_out[var])

        # Add chunks attribute
        f_out[var].attrs['chunks'] = chunks
    
        # Fix scale_factor
        if 'scale_factor' in f_out[var].attrs.keys():
            f_out[var].attrs['scale_factor'] = 1 / f_out[var].attrs['scale_factor']

        # Attach scales to the dims
        f_out[var].dims[0].attach_scale(f_out['time_index'])
        f_out[var].dims[1].attach_scale(f_out['gid'])

        # Progress report
        logging.info(f'{elapsed_time(st)} - {file_name}: Done!')

    logging.info(f'{elapsed_time(st)} - {file_name}: All variables transformed!')

    # Start tracking identical_dims (anything with only a gid dimension)
    identical_dims = ['gid']

    # Unpack metadata variables
    for var in f_in['meta'].dtype.names:
        logging.info(f'{elapsed_time(st)} - {file_name}: Unpacking {var} from meta...')
        element_size = f_in['meta'][var].dtype.itemsize
        gid_chunk_len = min(chunk_size * 2 ** 20 // element_size, nloc)
        chunks = (gid_chunk_len,)
        f_out.create_dataset(var, data=f_in['meta'][var], chunks=chunks)

        # Add chunks attribute
        f_out[var].attrs['chunks'] = chunks

        # Attach dimension scales to the dimensions
        f_out[var].dims[0].attach_scale(f_out['gid'])

        # Append to identical_dims
        identical_dims.append(var)
        
        logging.info(f'{elapsed_time(st)} - {file_name}: Done!')
    
    logging.info(f'{elapsed_time(st)} - {file_name}: meta unpacked!')

    # Add identical_dims to file metadata so we can pass to kerchunk later
    f_out.attrs['identical_dims'] = identical_dims

    # Close the datasets to ensure changes are written
    f_in.close()
    f_out.close()

    logging.info(f'{elapsed_time(st)} - {file_name}: Done with transormations!')

    return

def transform_sup3rcc_h5_file(infile, outfile):
    # This function is designed to transform h5 files for the Sup3rcc dataset, to prepare them for use with Kerchunk.
    # infile and outfile should both be local file paths. infile is the original Sup3rcc h5 file. outfile will be created
    # by copying and transforming the data from infile.

    # The Sup3rcc data uses a nice rectangular, evenly-spaced grid of lon/lat coordinates. This allowed for easy transformation
    # from 2 dimensions to 3 dimensions, which results in improved user experience when loading the data with xarray.

    # Summary of data transformations:

    # 1. time_index is converted from byte-string to int (when read by xarray, this will automatically convert to np.datetime64)
    # 2. latitude and longitude are given their own datsets
    # 3. time_index, latitude and longitude are converted to dimension scales
    # 4. Each variable is reshaped from 2 dims (time_index, location) to 3 dims (time_index, latitude, longitude)
    # 5. Each variable is rechunked, resulting in about 1.8 MB per chunk
    # 6. The dimension scales are attached to each variable's dimensions
    # 7. The scale_factor metadata is inverted (new_sf = 1 / old_sf)

    # TODO
    # 1. Future iterations of this transformation should modify the original h5 file, rather than copying the contents to a new file
    # 2. Rechunking should be automated (currently the choice of chunk size is specific to the Sup3rcc dataset) 

    # Open infile, create outfile
    f1 = h5py.File(infile)
    f2 = h5py.File(outfile, 'a')

    # Copy attributes
    for attr in f1.attrs.keys():
        f2.attrs[attr] = f1.attrs[attr]

    # Get the length of time_index
    time_len = f1['time_index'].len()

    # Convert time_index from  bytes to float.
    t = pd.Series(f1['time_index'])
    t = t.str.decode('utf8')
    t = t.str.split('+', expand=True)[0]
    t = np.array(t,dtype=np.datetime64)
    t = t.astype('int')

    # Grab the lat and lon coordinates from meta
    lat = f1['meta']['latitude'].reshape(650, 1475)[:, 0]
    lon = f1['meta']['longitude'].reshape(650, 1475)[0, :]

    # Add time_index dimension to the temp dataset. 'units' metadata required for xarray to interpret as datetime.
    f2.create_dataset('time_index', data=t)
    f2['time_index'].attrs['units'] = b'seconds since 1970-01-01'

    # Add lon/lat dimensions to temp dataset
    f2.create_dataset('latitude', data=lat)
    f2.create_dataset('longitude', data=lon)

    # Convert them to dimension scales
    f2['time_index'].make_scale()
    f2['latitude'].make_scale()
    f2['longitude'].make_scale()

    logging.info('Dimension scales created.')

    # Get var names
    vars = [var for var in f1.keys() if var not in ['meta', 'time_index']]

    # Loop over the variables and transfer them to the temp data set
    for var in vars:
        # Check dimensions
        time_len = f1['time_index'].len()
        assert f1[var].shape[0] == time_len
        assert f1[var].shape[1] == 650 * 1475

        # Copy data, reshape it and rechunk it. Now we have 3 dims, time, lat, lon
        # Note that chunks=True will result in auto-chunking. This doesn't really work when
        # data sets have different lengths for the time_index (as is the case for Sup3rcc)
        chunks = (24, 130, 295)
        f2.create_dataset(var, data=f1[var][:].reshape(time_len, 650, 1475), chunks=chunks)  # Results in 1.8 MB chunks for pressure data
        logging.info(f'{var} reshaped and transferred to new dataset.')

        # Add attributes
        for attr in f1[var].attrs.keys():
            if attr == 'scale_factor':
                f2[var].attrs[attr] = 1 / f1[var].attrs[attr]
            elif attr != 'chunks':
                f2[var].attrs[attr] = f1[var].attrs[attr]
        f2[var].attrs['chunks'] = chunks

        # Label the dimensions of the main variable
        f2[var].dims[0].label = 'time_index'
        f2[var].dims[1].label = 'latitude'
        f2[var].dims[2].label = 'longitude'

        # Attach dimension scales to the dimensions
        f2[var].dims[0].attach_scale(f2['time_index'])
        f2[var].dims[1].attach_scale(f2['latitude'])
        f2[var].dims[2].attach_scale(f2['longitude'])

        logging.info(f'Dimension scales attached to {var}.')

    # Add metadata variables
    for var in f1['meta'].dtype.names:
        if var not in ['latitude', 'longitude']:
            chunks = (130, 295)
            f2.create_dataset(var, data=f1['meta'][var].reshape(650, 1475), chunks=chunks)

            # Add chunks attribute
            f2[var].attrs['chunks'] = chunks

            # Label the dimensions of the main variable
            f2[var].dims[0].label = 'latitude'
            f2[var].dims[1].label = 'longitude'

            # Attach dimension scales to the dimensions
            f2[var].dims[0].attach_scale(f2['latitude'])
            f2[var].dims[1].attach_scale(f2['longitude'])

    # Close the new dataset to ensure changes are written
    f1.close()
    f2.close()

    return

def gen_ref(local_path, storage_path, ref_file=None):
    # local_path is the file to be analyzed. storage_path is the path to the same file in cloud storage. ref_file is
    # an optional argument that can be used to save the kerchunk references as a json.\

    with open(local_path, 'rb') as f:
        ref = SingleHdf5ToZarr(f, storage_path, inline_threshold=300).translate()
    
    if ref_file:
        with open(ref_file, 'wb') as f:
            f.write(ujson.dumps(ref).encode())

    return ref

def gen_ref_comb(refs, ref_file=None, concat_dims=['time_index'], identical_dims=None, remote_protocol='abfs'):
    # This function takes a list of kerchunk references and combines them into a single reference.
    # For sup3rcc, we used identical_dims=['country', 'county', 'eez', 'elevation', 'latitude', 'longitude', 'offshore', 'state', 'timezone'],
    # however, None would probably have been fine...
    # Generate combo reference
    
    if remote_protocol not in ['s3', 'abfs']:
        raise NotImplementedError()
    
    kwargs = {
        'remote_protocol': remote_protocol,
        'concat_dims': concat_dims,
        'identical_dims': identical_dims
    }
    if remote_protocol == 'abfs':
        token = planetary_computer.sas.get_token('nrel', 'oedi').token
        kwargs['remote_options'] = {'account_name': 'nrel', "credential": token}

    ref_comb = MultiZarrToZarr(refs, **kwargs).translate()

    # Write to json file
    if ref_file:
        with open(ref_file, 'wb') as f:
            f.write(ujson.dumps(ref_comb).encode())

    return ref_comb
