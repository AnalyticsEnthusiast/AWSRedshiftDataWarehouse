# Script that will spin up a new Redshift cluster if one does not exist
# Best practice so that resource are not left running in AWS
import boto3
import configparser
from botocore.exceptions import ClientError
import pandas as pd
import time

#Get credentials
#Get credentials
config = configparser.ConfigParser()
config.read('song_dwh.cfg')

KEY = config.get('AWS', 'key')
SECRET = config.get('AWS', 'secret')
ARN = config.get("ARN", "arn")

DWH_REGION = config.get("DWH", "dwh_region")
DWH_CLUSTER_TYPE = config.get("DWH", "dwh_cluster_type")
DWH_NUM_NODES = config.get("DWH","dwh_num_nodes")
DWH_NODE_TYPE = config.get("DWH","dwh_node_type")
DWH_IAM_ROLE_NAME = config.get("DWH", "dwh_iam_role_name")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","dwh_cluster_identifier")
DWH_DB = config.get("DWH","dwh_db")
DWH_DB_USER = config.get("DWH","dwh_db_user")
DWH_DB_PASSWORD = config.get("DWH","dwh_db_password")
DWH_PORT = config.get("DWH","dwh_port")

LOG_DATA = config.get('S3','log_data')
SONG_DATA = config.get('S3', 'song_data')


def update_arn():
    config = configparser.ConfigParser()
    config.read('song_dwh.cfg')
    
    config.set("ARN","ARN", "")
    with open("song_dwh.cfg", "w") as con:
        config.write(con)

# Spin down Redshift cluster if exists
def drop_cluster():
    redshift = boto3.client('redshift',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

# Drop s3 bucket read user 
# Remove IAM user
def remove_user():
    try:
        iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=DWH_REGION)
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        
        update_arn()
    except Exception as e:
        print(e)

    
def main():
    drop_cluster()
    remove_user()
    
    redshift = boto3.client('redshift',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    try:
        while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"] == "deleting":
            print("deleting cluster.....")
            time.sleep(60)
    except Exception as e:
        print("cluster deleted")
    
    
if __name__ == "__main__":
    main()