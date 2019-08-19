import logging
import time

logger = logging.getLogger(__name__)


class AWSIAMHelper():

    logger.propagate = False

    def __init__(self, session):
        '''
        Initialize the AWSIAM class with a boto3 Session
        :param session: boto3 Session from 'parent' job base class
        '''
        self.session = session
        self.iam = self.session.client('iam')

    def role_stitcher(self, role_name, trust_service, description, policies_list=[], managed_policie_arns=[]):
        '''
        Creates a role and attached the policies - will catch errors and skip if role already exists
        :param role_name: Name of service role to create
        :param trust_service: Trusted service to associate with the service role
        :param description: Description of role
        :param policies_list: List of JSON policies (optional)
        :param managed_policie_arns: Managed policies to attach (optional)
        :return: Role ARN is returned
        '''
        role_arn = None
        trust_policy = f'''{{
                        "Version": "2012-10-17",
                        "Statement": [{{
                            "Effect": "Allow",
                            "Principal": {{
                                "Service": "{trust_service}.amazonaws.com"
                            }},
                            "Action": "sts:AssumeRole"
                        }}]
                    }}
                '''

        try:
            response = self.iam.create_role(
                Path='/',
                RoleName=role_name,
                AssumeRolePolicyDocument=trust_policy,
                Description=description
            )
            role_arn = response['Role']['Arn']

            p_counter = 1
            for policy in policies_list:

                response = self.iam.put_role_policy(
                    RoleName=role_name,
                    PolicyName=f'{role_name}_policy_{p_counter}',
                    PolicyDocument=policy
                )
                p_counter = p_counter + 1

            for managed_policy_arn in managed_policie_arns:

                response = self.iam.attach_role_policy(
                    PolicyArn=managed_policy_arn,
                    RoleName=role_name
                )

            logger.info(f'Role {role_name} created')

            return role_arn

        except Exception as e:
            if 'EntityAlreadyExists' in str(e):
                logger.info(f'Role {role_name} not created - already exists')
                response = self.iam.get_role(
                    RoleName=role_name
                )
                role_arn = response['Role']['Arn']
                return role_arn

            else:
                raise

    def delete_role(self, role_name):
        '''
        Delete a role
        :param role_name: name of the role to delete
        :return: None
        '''
        try:
            response = self.iam.list_role_policies(
                RoleName=role_name
            )

            for policy in response['PolicyNames']:
                self.iam.delete_role_policy(
                    RoleName=role_name,
                    PolicyName=policy
                )

            response = self.iam.list_attached_role_policies(
                RoleName=role_name
            )

            for policy in response['AttachedPolicies']:
                self.iam.detach_role_policy(
                        RoleName=role_name,
                        PolicyArn=policy['PolicyArn']
                    )

            logger.info(f'Policies detached from role {role_name}.')

            response = self.iam.delete_role(
                RoleName=role_name
            )
            logger.info(f'Role {role_name} deleted.')
        except Exception as e:
            if 'NoSuchEntity' in str(e):
                logger.info(f'Role {role_name} missing, skipping...')
            else:
                raise

    def delete_instance_profile(self, instance_profile_name):

        try:
            self.iam.delete_instance_profile(
                InstanceProfileName=instance_profile_name
            )
            logger.info(f"Instance profile {instance_profile_name} deleted.")
        except Exception as e:
            if 'NoSuchEntity' in str(e):
                logger.info(f"Instance profile {instance_profile_name} missing, skipping...")
            else:
                raise

    def remove_role_from_instance_profile(self, instance_profile_name):
        try:
            response = self.iam.get_instance_profile(
                InstanceProfileName=instance_profile_name
            )

            for role in response['InstanceProfile']['Roles']:
                response = self.iam.remove_role_from_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    RoleName=role['RoleName']
                )
            logger.info(f"Roles removed from instance profile {instance_profile_name}")
        except Exception as e:
            if 'NoSuchEntity' in str(e):
                logger.info(f"Instance profile {instance_profile_name} does not exist. Skipping...")
            else:
                raise



class AwsBase(object):

    def __init__(self, boto3_session):
        self.boto3_session = boto3_session
        self.iam_helper = AWSIAMHelper(self.boto3_session)

        self.tracking_the_sun_bucket = "s3://oedi-dev-tracking-the-sun"
        self.tracking_the_sun_table_name = "oedi_tracking_the_sun"

        self.pv_rooftops_bucket = "s3://oedi-dev-pv-rooftop"
        self.pv_rooftops_rasd_table_name = "pv_rooftops_rasd"
        self.pv_rooftops_buildings_table_name = "pv_rooftops_buildings"
        self.pv_rooftops_dev_planes_table_name = "pv_rooftops_developable_planes"
        self.pv_rooftops_aspects_table_name = "pv_rooftops_aspects"
        self.rsf_array_bucket = "s3://oedi-rsf-array"
        self.rsf_array_table_name = 'nrel_rsf_array'
        self.stf_array_bucket = "s3://oedi-stf-array"
        self.stf_array_table_name = 'nrel_stf_array'
        self.garage_array_bucket = "s3://oedi-garage-array"
        self.garage_array_table_name = 'nrel_garage_array/'
        self.windsite_array_bucket = "s3://oedi-windsite-array"
        self.windsite_array_table_name = 'nrel_windsite_array'



