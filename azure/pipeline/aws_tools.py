import boto3
import ujson
import s3fs
from etl_tools import load_oedi_sas, gen_ref_comb
from azure.storage.blob import ContainerClient
from dotenv import load_dotenv
import subprocess
import h5py

def get_tags(org, billingid, task, owner):
    tags = [
        {
            'key': 'org',
            'value': org
        },
        {
            'key': 'billingid',
            'value': billingid
        },
        {
            'key': 'task',
            'value': task
        },
        {
            'key': 'owner',
            'value': owner
        }
    ]
    return tags

def get_dataset(bucket, prefix=None, extension='.h5', resolution=None):
    """
    This is a convenience function that generates a list of s3 bucket+key paths for a given dataset

    Parameters
    ----------
    bucket : str (required)
        Bucket in which the dataset lives (e.g. 'nrel-pds-wtk')
    prefix : str
        Prefix of all files in the dataset (e.g. 'Great_Lakes')
    extension : str
        File extension for all files in the dataset (e.g. '.h5')
    resolution : str
        For WIND data only. Options are 'hourly' or '5min'

    Returns
    -------
    files : list
        List of all bucket+key paths to all files in the bucket subject to the provided options.
    """
    s3 = s3fs.S3FileSystem(anon=True)
    with open('aws_glob_patterns.json') as f:
        aws_glob_patterns = ujson.load(f)

    if prefix and resolution:
        files = s3.glob(aws_glob_patterns[bucket][prefix][resolution])
    elif prefix:
        files = s3.glob(aws_glob_patterns[bucket][prefix])
    else:
        files = s3.glob(aws_glob_patterns[bucket])
    
    files = [file for file in files if file.endswith(extension)]
    
    return files

def get_StepFunctionRole():
    # TODO: Add code that creates the role if it doesn't exist
    """
    This function obtains the StepFunctionRole to be used when creating a step function.

    Parameters
    ----------
    None

    Returns
    -------
    roleArn : str
        Amazon resource number of the StepFunctionRole
    """
    iam = boto3.client('iam')
    roleArn = iam.get_role(RoleName='StepFunctionRole')['Role']['Arn']
    return roleArn

def create_state_machine(name, definition='./ASL/state_machine_template.json', tags=None, region_name='us-west-2'):
    """
    This is a convenience function that creates or updates a state machine in AWS from the definition.

    Parameters
    ----------
    name : str (required)
        The name given to the state machine in AWS.
    definition : str
        Path to the json file that contains the ASL definition of the state machine.
    tags : dict
        key-value pairs for tracking aws resources. Defaults will be used if none are provided (see get_tags).
    Returns
    -------
    stateMachineArn : str
        The amazon resource number of the state machine.
    """
    
    sf = boto3.client('stepfunctions', region_name=region_name)
    
    sms = sf.list_state_machines()['stateMachines']
    stateMachineArn = ''
    for sm in sms:
        if sm['name'] == name:
            stateMachineArn = sm['stateMachineArn']
            break
    
    if not tags:
        tags = get_tags()

    with open(definition) as f:
        if stateMachineArn:
            sf.update_state_machine(stateMachineArn=stateMachineArn, definition=f.read())
        else:
            roleArn = get_StepFunctionRole()
            stateMachineArn = sf.create_state_machine(name=name, definition=f.read(), roleArn=roleArn, tags=tags)['stateMachineArn']
    return stateMachineArn

def get_state_machine(name, region_name='us-west-2'):
    """
    This function gets the ARN for a state machine by name.

    Parameters
    ----------
    name : str (required)
        The name of the state machine in AWS.

    Returns
    -------
    stateMachineArn : str
        The amazon resource number of the state machine.
    """
    sf = boto3.client('stepfunctions', region_name=region_name)
    sms = sf.list_state_machines()['stateMachines']
    stateMachineArn = ''
    for sm in sms:
        if sm['name'] == name:
            stateMachineArn = sm['stateMachineArn']
            break
    if not stateMachineArn:
        raise Exception(f'State machine {name} not found.')
    return stateMachineArn

def create_state_machine_input(files, staging_bucket, s3_comb_ref_file, az_comb_ref_file, run_name=None, input_file='ASL/state_machine_input.json'):
    # TODO: Check access/existence to/of staging bucket
    """
    This function generates the state machine input to process a dataset.

    Parameters
    ----------
    files : list (required)
        A list of bucket+key paths to the files of the dataset
    staging_bucket : str
        Name of the bucket where transformed files and json references will be written
    s3_comb_ref_file : str
        Key for the combined kerchunk reference file that points to the dataset in staging
    az_comb_ref_file : str
        Key for the combined kerchunk reference file that points to the dataset in azure
    run_name : str
        The name of the run. This will be used to create a json file in S3 containing the inputs needed for the run
    input_file : str
        A path in which to store a local copy of the json inputs needed for the run.
    Returns
    -------
    input_data : str
        A serialized copy of the input data
    """
    smi = {
        's3_files': files,
        'staging_bucket': staging_bucket,
        's3_comb_ref_file': s3_comb_ref_file,
        'az_comb_ref_file': az_comb_ref_file,
        'run_name' : run_name
    }
    with open(input_file, 'w') as f:
        ujson.dump(smi, f)

    s3 = s3fs.S3FileSystem()
    s3.put_file(input_file, f'{staging_bucket}/{run_name}.json')

    input_data = ujson.dumps(smi)
    return input_data

def run_state_machine(name, run_name = 'sm_run', input_file='ASL/state_machine_input.json', region_name='us-west-2'):
    sf = boto3.client('stepfunctions', region_name=region_name)
    stateMachineArn = get_state_machine(name)
    with open(input_file) as f:
        input = f.read()
    response = sf.start_execution(stateMachineArn=stateMachineArn, name=run_name, input=input)
    return response

def create_job_def(job_def_file='./ASL/job_definition.json', region_name='us-west-2'):
    with open(job_def_file) as f:
        job_def = ujson.load(f)
    tags = get_tags()
    job_def['tags'] = {}
    for tag in tags:
        job_def['tags'][tag['key']] = tag['value']
    job_def['propagateTags'] = True
    batch = boto3.client('batch', region_name=region_name)
    response = batch.register_job_definition(**job_def)
    return response

def create_launch_templates():
    # TODO: Need to add the 2TB and 3TB versions
    ec2 = boto3.client('ec2')
    LaunchTemplateNames = ['kerchunk-1TB']
    for LaunchTemplateName in LaunchTemplateNames:
        with open(f'./ASL/{LaunchTemplateName}.json') as f:
            LaunchTemplateData = ujson.load(f)
        existing_template = ec2.describe_launch_templates(Filters=[{'Name': 'launch-template-name', 'Values': [LaunchTemplateName]}])
        if existing_template:
            ec2.create_launch_template_version(LaunchTemplateName=LaunchTemplateName, LaunchTemplateData=LaunchTemplateData)
        else:
            ec2.create_launch_template(LaunchTemplateName=LaunchTemplateName, LaunchTemplateData=LaunchTemplateData)

def create_cluster():
    batch = boto3.client('batch')

def create_aws_resources():
    create_state_machine('kerchunk-h5')
    create_job_def()

def process_h5_dataset(files, staging_bucket, s3_comb_ref_file, az_comb_ref_file, state_machine_name='kerchunk_h5', region_name='us-west-2'):
    smi = create_state_machine_input(files, staging_bucket, s3_comb_ref_file, az_comb_ref_file)
    stateMachineArn = get_state_machine(state_machine_name)
    sf = boto3.client('stepfunctions', region_name=region_name)
    sf.start_execution(stateMachineArn=stateMachineArn, input=smi)

def copy_s3_dataset_to_azure(files, staging_bucket, dry_run=False):
    CONTAINER_NAME = 'oedi'
    sas = load_oedi_sas()
    load_dotenv()   # Store AWS credentials in .env file
    cmd = [
        'azcopy',
        'copy',
        f'https://s3.us-west-2.amazonaws.com/{staging_bucket}',
        f'https://nrel.blob.core.windows.net/{CONTAINER_NAME}?{sas}',
        '--include-path',
        ';'.join(files)
    ]

    if dry_run:
        cmd.append('--dry-run')

    subprocess.run(cmd)

def create_combined_ref(files, staging_bucket, comb_ref_file=None, remote_protocol='s3'):
    s3 = s3fs.S3FileSystem()
    f = h5py.File(s3.open(f'{staging_bucket}/{files[0]}'))
    identical_dims = list(f.attrs['identical_dims'])
    if remote_protocol == 's3':
        ref_files = [file.replace('.h5', '_s3.json') for file in files]
    elif remote_protocol == 'abfs':
        ref_files = [file.replace('.h5', '.json') for file in files]
    else:
        raise Exception('remote_protocol must be "s3" or "abfs"')
    refs = []
    for ref_file in ref_files:
        with s3.open(f'{staging_bucket}/{ref_file}', 'rb') as f:
            refs.append(ujson.load(f))
    ref_comb = gen_ref_comb(refs, identical_dims=identical_dims, remote_protocol=remote_protocol)
    temp_file = 'temp.json'
    with open(temp_file, 'wb') as f:
        f.write(ujson.dumps(ref_comb).encode())
    s3.put_file(temp_file, f's3://{staging_bucket}/{comb_ref_file}')
    if remote_protocol=='abfs':
        sas = load_oedi_sas()
        CONTAINER_NAME = 'oedi'
        dest = f'https://nrel.blob.core.windows.net/{CONTAINER_NAME}/{comb_ref_file}?{sas}'
        subprocess.run(['azcopy', 'copy', temp_file, dest])

def copy_s3_file_to_azure(source, dest, sas=None, container='oedi'):
    s3 = s3fs.S3FileSystem()
    if not sas:
        sas = load_oedi_sas()
    client = ContainerClient.from_container_url(f'https://nrel.blob.core.windows.net/{container}?{sas}')
    blob = client.get_blob_client(dest)
    with s3.open(source, 'rb') as f:
        blob.upload_blob(f.read())

def copy_local_file_to_azure(source, dest, sas=None, container='oedi'):
    if not sas:
        sas = load_oedi_sas()
    client = ContainerClient.from_container_url(f'https://nrel.blob.core.windows.net/{container}?{sas}')
    blob = client.get_blob_client(dest)
    with open(source, 'rb') as f:
        blob.upload_blob(f.read())
