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
#config.read('song_dwh.cfg')
config.read('dwh.cfg')

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

def update_endpoint():
    """
    Description: Updates the dwh.cfg file and removes the dwh_endpoint value.
    
    Arguments:
        None
    Output:
        None
    """
    config = configparser.ConfigParser()
    #config.read('song_dwh.cfg')
    config.read('dwh.cfg')
    
    config.set("DWH","dwh_endpoint", "")
    
    with open("dwh.cfg", "w") as con:
        config.write(con)
    
    #with open("song_dwh.cfg", "w") as con:
    #    config.write(con)
    
def update_arn():
    """
    Description: Updates the dwh.cfg file and removes the ARN value.
    
    Arguments:
        None
    Output:
        None
    """
    config = configparser.ConfigParser()
    #config.read('song_dwh.cfg')
    config.read('dwh.cfg')
    
    config.set("ARN","arn", "")
    
    with open("dwh.cfg", "w") as con:
        config.write(con)
    
    #with open("song_dwh.cfg", "w") as con:
    #    config.write(con)
    

def drop_cluster():
    """
    Description: Drops the Redshift cluster if the endpoint exists.
    
    Arguments:
        None
    Output:
        None
    """
    redshift = boto3.client('redshift',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    
def remove_user():
    """
    Description: Drops the Redshift cluster ARN user that can read from S3.
    
    Arguments:
        None
    Output:
        None
    """
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
    """
    Description: Main processing loop.
    
    Arguments:
        None
    Output:
        None
    """
    drop_cluster()
    remove_user()
    update_endpoint()
    
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