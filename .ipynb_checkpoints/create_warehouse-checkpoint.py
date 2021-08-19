# Script that will spin up a new Redshift cluster if one does not exist
# Best practice so that resource are not left running in AWS
import boto3
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')


# Create user (if not exist) that can read from S3 and load Redshift

# Create a redshift Security Group if it does not exist

# Create new redshift cluster (if not exist)

