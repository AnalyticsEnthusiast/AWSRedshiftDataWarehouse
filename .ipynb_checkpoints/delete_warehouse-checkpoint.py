# Script that will spin up a new Redshift cluster if one does not exist
# Best practice so that resource are not left running in AWS
import boto3
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')


# Spin down Redshift cluster if exists

# Drop security group if exists

# Drop s3 bucket read user 
