import os
import h5py
import subprocess
import math
from glob import glob
import re

def run_job(job_file):
    job_submission = subprocess.run(['sbatch', job_file], capture_output=True)
    output = job_submission.stdout.decode()
    if 'Submitted batch job ' in output:
        jobid = output.split()[3]
    else:
        jobid = 0
        print(f'Job submission failure: {job_submission.stderr.decode()}')
    return jobid

def cancel_jobs(job_ids):
    for job_id in job_ids:
        subprocess.run(['scancel', job_id])

def construct_paths(file):
    # Need username to access scratch
    user = os.getenv('USER')
    
    file_name = file.split('/')[-1]
    job_name = file_name.replace('.h5', '')
    job_dir = file.replace('/datasets', f'/scratch/{user}').replace(file_name, '')
    ref_file = f'{job_dir}{job_name}.json'

    if 'WIND' in file:
        az_path = file.replace('/datasets/WIND', 'wtk')
    elif 'sup3rcc' in file:
        az_path = file.replace('/datasets/', '')
    else:
        raise NotImplementedError(f'The only Eagle datasets that have been implemented are WIND and sup3rcc.')

    return file_name, job_name, job_dir, ref_file, az_path

def get_dep_str(dependency):
    if not isinstance(dependency, (list, tuple)):
        dependency = [dependency, ]

    return '#SBATCH --dependency=afterok:' + ':'.join([str(id) for id in dependency])

def get_dataset(dataset, resolution=None):
    files = []
    if 'WIND' in dataset:
        subsets = ['North_Atlantic', 'gulf_of_mexico']
        subsets2 = ['india']
        if any([subset in dataset for subset in subsets]):
            if resolution == 'hourly':
                files = glob(f'/datasets/{dataset}/yearly_hr/*.h5')
            elif resolution == '5min':
                files = glob(f'/datasets/{dataset}/yearly/*.h5')
        elif any([subset in dataset for subset in subsets2]):
            if resolution == '5min':
                files = glob(f'/datasets/{dataset}/*.h5')
            else:
                files = []
        else:
            if resolution == 'hourly':
                files = glob(f'/datasets/{dataset}/*.h5')
            elif resolution == '5min':
                files = glob(f'/datasets/{dataset}/*/*.h5')
            else:   # 10min and 15min resolutions
                files = glob(f'/datasets/{dataset}/*.h5')
    return files

def gen_hpc_single_job(file, job_dir, job_name, mem_GB=None, time_limit_hrs=4, debug=False):

    # Get bash path
    bash_path = os.popen('which bash').read().replace('\n', '')

    # Construct paths
    file_name, job_name, job_dir, ref_file, az_path = construct_paths(file)

    # Get user
    user = os.getenv('USER')

    # Set parameters
    nodes = 1
    ntasks = 1

    # Create job file paths
    job_file = f'{job_dir}{job_name}.sh'
    output_file = f'{job_dir}{job_name}_out'
    error_file = f'{job_dir}{job_name}_err'

    # Add debug partition if desired
    if debug:
        add_debug = '#SBATCH --partition=debug'
        time_limit_hrs = 1
    else:
        add_debug = ''

    if mem_GB:
        add_mem = f'#SBATCH --mem={mem_GB}GB'
    else:
        add_mem = ''

    with open(job_file, 'w') as f:
        # Write SBATCH inputs
        f.write(
f"""#!{bash_path}
#SBATCH --job-name='{job_name}'
#SBATCH --nodes={nodes}
#SBATCH --ntasks={ntasks}
#SBATCH --time={time_limit_hrs:.0f}:00:00
#SBATCH -o {output_file}
#SBATCH -e {error_file}
#SBATCH --export=ALL
#SBATCH --account=oedi
{add_mem}
{add_debug}

#------------------

cd /scratch/$USER
module load conda
conda activate .env2
srun python /home/{user}/oedi_azure/pipeline/hpc_process_file.py {file}
"""
        )

    return job_file

def gen_hpc_combine_refs_job(comb_ref_file, ref_files, time_limit_hrs=4, dependency=None, debug=False, py_file='/home/mheine/oedi_azure/pipeline/hpc_gen_refs.py'):
    bash_path = os.popen('which bash').read().replace('\n', '')
    
    comb_ref_file_name = comb_ref_file.split('/')[-1]
    job_dir = comb_ref_file.replace(comb_ref_file_name, '')
    job_name = comb_ref_file_name.replace('.json', '')
    job_file = f'{job_dir}{job_name}.sh'

    # Add dependency if any
    if dependency:
        add_dependency = get_dep_str(dependency)
    else:
        add_dependency = ''

    # Add debug partition if desired
    if debug:
        add_debug = '#SBATCH --partition=debug'
        time_limit_hrs = 1
    else:
        add_debug = ''

    # Create job file
    with open(job_file, 'w') as f:
        # Write SBATCH inputs
        f.write(
f"""#!{bash_path}
#SBATCH --job-name='{job_name}'
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time={time_limit_hrs:.0f}:00:00
#SBATCH -o {job_dir}{job_name}_out
#SBATCH -e {job_dir}{job_name}_err
#SBATCH --export=ALL
#SBATCH --account=oedi
{add_dependency}
{add_debug}

#------------------
cd /scratch/$USER
module load conda
conda activate .env2
srun python {py_file} {comb_ref_file} {' '.join(ref_files)}

"""
        )
    return job_file

def gen_hpc_to_azure_job(files, transformed_files, az_paths, dependency=None, transfer_speed=1500, debug=False, py_file='/home/mheine/oedi_azure/pipeline/hpc_to_azure.py'):
    # Transfer speed in Mb/s
    bash_path = os.popen('which bash').read().replace('\n', '')

    first_file_name = transformed_files[0].split('/')[-1]
    job_dir = transformed_files[0].replace(first_file_name, '')
    match = re.search(r'/\d\d\d\d/$', job_dir)
    if match:
        year = match.group(0)
        job_dir = job_dir.replace(year, '/')
    job_name = 'hpc_to_azure'
    existing_job_files = glob(job_dir + 'hpc_to_azure*.sh')
    if existing_job_files:
        job_name += f'_{len(existing_job_files) + 1}'
    job_file = f'{job_dir}{job_name}.sh'

    # Estimate time requirements
    
    total_bytes = 0
    for file in files:
        total_bytes += os.stat(file).st_size
    
    time_factor = 1.5   # Provide extra time in case things move a little slower than usual
    time_required_hrs = math.ceil(time_factor * total_bytes * 8 * 10 ** -6 / transfer_speed / 60 / 60)
    if time_required_hrs > 240:
        print('Warning: Transfer job is estimated to take longer than the maximum of 240 hrs.')
        time_required_hrs = 240

    # Create transfer args
    # <source>:<destination>
    transfer_args = [f'{transformed_file}:{az_path}' for transformed_file, az_path in zip(transformed_files, az_paths)]

    # Add dependency if any
    if dependency:
        add_dependency = get_dep_str(dependency)
    else:
        add_dependency = ''

    # Add debug partition if desired
    if debug:
        add_debug = '#SBATCH --partition=debug'
        time_required_hrs = 1
    else:
        add_debug = ''

    # Create job file
    with open(job_file, 'w') as f:
        # Write SBATCH inputs
        f.write(
f"""#!{bash_path}
#SBATCH --job-name='{job_name}'
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time={time_required_hrs:.0f}:00:00
#SBATCH -o {job_dir}{job_name}_out
#SBATCH -e {job_dir}{job_name}_err
#SBATCH --export=ALL
#SBATCH --account=oedi
{add_dependency}
{add_debug}

#------------------
cd /scratch/$USER
module load conda
conda activate .env2
srun python {py_file} {' '.join(transfer_args)}

"""
        )
    return job_file

def process_h5_dataset(files, comb_ref_file=None, time_limit_hrs=None, mem_factor=1.2, debug=False, skip_transformation=False, skip_transfer_to_azure=False):
    # For each file in files, we generate a job script and submit to sbatch
    # files should a be a list of absolute file paths to files in the /datasets directory.

    # Make lists to track jobs
    job_ids = []
    ref_files = []
    transformed_files = []
    az_paths = []
    # Loop over files
    print(f'Starting {len(files)} transformation jobs.')
    for file in files:
        # Get max dataset size to determine memory allocation
        # f = h5py.File(file)
        # max_dataset_size = 0
        # for key in f.keys():
        #     max_dataset_size = max(max_dataset_size, f[key].nbytes * 10 ** -9)
        # mem_GB = int(max_dataset_size * mem_factor)

        # It was found that files as small as 415 GB timed out when only given 4 hours.
        # In practice, there is a lot of variablity in the lengths of job runs. This may
        # be due to network limitations when running many jobs concurrently. We're bumping
        # up the time limit to 48 (the limit for the standard partition) for all files larger
        # than 400 GB.
        if not time_limit_hrs:
            # Get file size to adjust time limit
            file_size_GB = os.stat(file).st_size * 10 ** -9
            if file_size_GB < 400:
                time_limit_hrs = 4
            else:
                time_limit_hrs = 48

        # Construct paths and create directory
        file_name, job_name, job_dir, ref_file, az_path = construct_paths(file)
        os.makedirs(job_dir, exist_ok=True)
        ref_files.append(ref_file)
        transformed_files.append(f'{job_dir}{file_name}')
        az_paths.append(az_path)

        if not skip_transformation:
            # Generate job file to transform and generate references for a single h5 file
            job_file = gen_hpc_single_job(file, job_dir, job_name, time_limit_hrs=time_limit_hrs, debug=debug)

            # Run job file
            job_id = run_job(job_file)
            if job_id == 0:
                cancel_jobs(job_ids)
                raise Exception('Job submission failure')
            else:
                job_ids.append(job_id)
    
    # Generate job file to copy dataset to Azure
    if not skip_transfer_to_azure:
        print('Starting job to copy dataset to Azure.')
        copy_job_file = gen_hpc_to_azure_job(files, transformed_files, az_paths, dependency=job_ids, debug=debug)
        copy_job_id = run_job(copy_job_file)
        if copy_job_id == 0:
            cancel_jobs(job_ids)
            raise Exception('Copy job submission failure')
        else:
            job_ids.append(copy_job_id)
    else:
        copy_job_id = None

    # Generate job file to combine references
    # NOTE THAT DEBUG IS CURRENTLY SET TO TRUE TO EXPEDITE JOBS WHILE ACCOUNT IN STANDBY
    print('Starting job to combine references.')
    if comb_ref_file:
        ref_job_file = gen_hpc_combine_refs_job(comb_ref_file, ref_files, dependency=copy_job_id, debug=True)
        ref_job_id = run_job(ref_job_file)
        if ref_job_id == 0:
            cancel_jobs(job_ids)
            raise Exception('Gen combined ref job submission failure')
        else:
            job_ids.append(ref_job_id)
    
    print('All jobs scheduled!')

    comb_ref_file_name = comb_ref_file.split('/')[-1]
    if 'hourly' in comb_ref_file_name:
        job_id_file = comb_ref_file.replace(comb_ref_file_name, 'job_ids_hourly.txt')
    elif '5min' in comb_ref_file_name:
        job_id_file = comb_ref_file.replace(comb_ref_file_name, 'job_ids_5min.txt')
    else:
        job_id_file = comb_ref_file.replace(comb_ref_file_name, 'job_ids.txt')
    with open(job_id_file, 'w') as f:
        f.writelines([job_id + '\n' for job_id in job_ids ])

    return job_ids

def process_h5_redos(files, redos, comb_ref_file=None, time_limit_hrs=None, debug=False, skip_transfer_to_azure=False):
    """
    Process an h5 datset where some of the transformations failed.

    Parameters
    ----------
    files : list
        Paths to source h5 files for entire dataset (must be in /datasets on Eagle)
    redos: list
        Paths to source h5 files that failed (must be in /datasets on Eagle)
    comb_ref_file: str
        Path to where the combined kerchunk reference file will be
        stored. If None, then no combined reference will be generated.
    time_limit_hrs: int
        Override the default time limit for the file transformation tasks.
    debug: bool
        Submit all jobs to the debug partition
    skip_transfer_to_azure: bool
        If true, then no files will be transferred to Azure.

    Returns
    -------
    job_ids : list
        List of all job_ids submitted to sbatch.
    """

    # For each file in redos, we generate a job script and submit to sbatch.

    # Make lists to track jobs
    job_ids = []
    ref_files = []
    transformed_files = []
    az_paths = []
    # Loop over files
    print(f'Starting {len(redos)} transformation jobs.')
    for file in files:

        # It was found that files as small as 415 GB timed out when only given 4 hours.
        # In practice, there is a lot of variablity in the lengths of job runs. This may
        # be due to network limitations when running many jobs concurrently. We're bumping
        # up the time limit to 48 (the limit for the standard partition) for all files larger
        # than 400 GB.
        if not time_limit_hrs:
            # Get file size to adjust time limit
            file_size_GB = os.stat(file).st_size * 10 ** -9
            if file_size_GB < 400:
                time_limit_hrs = 4
            else:
                time_limit_hrs = 48

        # Construct paths and create directory
        file_name, job_name, job_dir, ref_file, az_path = construct_paths(file)
        os.makedirs(job_dir, exist_ok=True)
        ref_files.append(ref_file)
        transformed_files.append(f'{job_dir}{file_name}')
        az_paths.append(az_path)

        if file in redos:
            # Generate job file to transform and generate references for a single h5 file
            job_file = gen_hpc_single_job(file, job_dir, job_name, time_limit_hrs=time_limit_hrs, debug=debug)

            # Run job file
            job_id = run_job(job_file)
            if job_id == 0:
                cancel_jobs(job_ids)
                raise Exception('Job submission failure')
            else:
                job_ids.append(job_id)

    # Generate job file to copy dataset to Azure
    if not skip_transfer_to_azure:
        print('Starting job to copy dataset to Azure.')
        copy_job_file = gen_hpc_to_azure_job(files, transformed_files, az_paths, dependency=job_ids, debug=debug)
        copy_job_id = run_job(copy_job_file)
        if copy_job_id == 0:
            cancel_jobs(job_ids)
            raise Exception('Copy job submission failure')
        else:
            job_ids.append(copy_job_id)
    else:
        copy_job_id = None

    # Generate job file to combine references
    if comb_ref_file:
        print('Starting job to combine references.')
        ref_job_file = gen_hpc_combine_refs_job(comb_ref_file, ref_files, dependency=copy_job_id, debug=debug)
        ref_job_id = run_job(ref_job_file)
        if ref_job_id == 0:
            cancel_jobs(job_ids)
            raise Exception('Gen combined ref job submission failure')
        else:
            job_ids.append(ref_job_id)
    
    print('All jobs scheduled!')
    
    comb_ref_file_name = comb_ref_file.split('/')[-1]
    if 'hourly' in comb_ref_file_name:
        job_id_file = comb_ref_file.replace(comb_ref_file_name, 'job_ids_hourly.txt')
    elif '5min' in comb_ref_file_name:
        job_id_file = comb_ref_file.replace(comb_ref_file_name, 'job_ids_5min.txt')
    else:
        job_id_file = comb_ref_file.replace(comb_ref_file_name, 'job_ids.txt')
    with open(job_id_file, 'w') as f:
        f.writelines([job_id + '\n' for job_id in job_ids ])

    return job_ids

def scan_err(dataset='WIND/Great_Lakes', resolution='5min'):
    if resolution == 'hourly':
        files = glob(f'/datasets/{dataset}/*.h5')
    elif resolution == '5min':
        files = glob(f'/datasets/{dataset}/*/*.h5')

    if len(files) == 0:
        raise Exception('No output files found. Dataset/resolution does not exists or has not been processed.')

    timeouts = []
    other_errors = []
    timeout_redos = []
    other_redos = []
    for file in files:
        err = file.replace('/datasets', '/scratch/mheine').replace('.h5', '_err')
        with open(err) as f:
            text = f.read()
            if 'TIME LIMIT' in text:
                timeouts.append(err)
                timeout_redos.append(file)
            elif len(text) > 0:
                other_errors.append(err)
                other_redos.append(file)
    print(f'Timeouts: {len(timeouts)}')
    print(f'Other errors: {len(other_errors)}')
    print(f'Total Files: {len(files)}')

    sizes = []
    for redo in timeout_redos:
        sizes.append(os.stat(redo).st_size * 10 ** -9)
    if len(sizes) > 0:
        print(f'The smallest file that timed out in {dataset} was {min(sizes):.0f} GB.')

    return files, timeout_redos, other_redos