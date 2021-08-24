# Script that will spin up a new Redshift cluster if one does not exist
# Best practice so that resource are not left running in AWS
import boto3
import configparser
from botocore.exceptions import ClientError
import pandas as pd
import time
import json

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


def update_endpoint(endpoint, port, db_name):
    
    config = configparser.ConfigParser()
    config.read('song_dwh.cfg')
    
    endpoint = endpoint + ":" + port + "/" + db_name
    
    config.set("DWH","dwh_endpoint", endpoint)
    
    with open("song_dwh.cfg", "w") as con:
        config.write(con)

        
def update_arn(ARN):
        
    config = configparser.ConfigParser()
    config.read('song_dwh.cfg')
    
    config.set("ARN","arn", ARN)
    
    with open("song_dwh.cfg", "w") as con:
        config.write(con)


def create_dwhuser():
    # Create iam client
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=DWH_REGION)

    try:
        # Check to see if the user exists, if not then create it
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    except Exception as e:

        try:
            dwhRole = iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )    
        except Exception as e:
            print(e)

        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
        
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        update_arn(roleArn)
    

def getroleArn():
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=DWH_REGION)
    
    return iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

    
def create_redshift_cluster():
    
    redshift = boto3.client('redshift',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    roleArn = getroleArn()
    print(roleArn)
    
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
        
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        prettyRedshiftProps(myClusterProps)
    
    except Exception as e:
        print(e)


def create_tcp_route():
    
    redshift = boto3.client('redshift',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"].lower() != "available":
        print("sleeping 60 sec......")
        time.sleep(60)
    
    ec2 = boto3.resource('ec2',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    
    endpoint = myClusterProps['Endpoint']['Address']
    print(endpoint)
    update_endpoint(endpoint, DWH_PORT, DWH_DB)
    
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    
        
    
def main():
    create_dwhuser()
    create_redshift_cluster()
    create_tcp_route()
    print("Redshift cluster has been created")

if __name__ == "__main__":
    main()

