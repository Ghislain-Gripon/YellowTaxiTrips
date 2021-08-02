import json
import boto3
import botocore
import pathlib
import urllib, yaml, logging, base64
import psycopg2
from botocore.exceptions import ClientError

def move_directory(key, bucket, directory):
    try:
        raw_key = pathlib.Path(key).name
        s3 = boto3.resource(
            service_name='s3',
            region_name="eu-west-3"
        )
        
        copy_source = {
          'Bucket': bucket,
          'Key': key
        }
        
        s3.Object(bucket, '{}/{}'.format(directory, raw_key)).copy_from(CopySource=copy_source)
        
        s3.Object(bucket, key).delete()
        
        return '{}/{}/{}'.format(bucket, directory, raw_key)
        
    except Exception as e:
        print("Error on move_directory({},{},{})".format(key, bucket, directory))
        print(e)
        raise e
        
def get_secret():
    secret_name = "redshiftqueryeditor-ggripon-taxitrip_cluster_secret"
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name= 'secretsmanager',
        region_name= "eu-west-3"
    )
    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        secret = None
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = str(base64.b64decode(get_secret_value_response['SecretBinary']))
        return secret
        
def read_yaml(file_stream) -> dict:
    _file = None
    try:
        _file = yaml.load(file_stream, Loader=yaml.SafeLoader)
        logging.info("YAML configuration file successfully read.")
    #catch a yaml related error to inform user of problem with config file
    
    except Exception as e:
        logging.error(str(e) + ", {}".format(file_stream))
        _file = None
        raise e
    return _file
        
def load(file_path):
    file_streaming = None
    raw_keys = pathlib.Path(urllib.parse.unquote(file_path)).parts
    directory:str = raw_keys[1]
    raw_key:str = raw_keys[2]
    bucket:str = raw_keys[0]
    
    try:
        s3 = boto3.client(service_name='s3',region_name='eu-west-3')
        file_streaming = s3.get_object(
            Bucket= bucket,
            Key= '{}/{}'.format(directory, raw_key)
            )['Body']
            
    except Exception as e:
        logging.error("Error on load({})".format(file_path))
        logging.error(e)
        raise e
    
    if pathlib.Path(raw_key).suffix == '.yaml':
        return read_yaml(file_streaming)
    else:
        return file_streaming.read()

def lambda_handler(event, context):
    #file_path = None
    try:
        
        print(load("postgretaxiconfig/config/config.yaml"))
        return {
            'HTMLCode': 200
        }
        #print("platform :" + str(platform.platform()))
        #print("system :" + str(platform.system()))
        #print("architecture :" + str(platform.architecture()))
        #s3 = boto3.client(service_name='s3',region_name='eu-west-3')
        #s3.copy_object(
        #    Bucket= event['Records'][0]['s3']['bucket']['name'],
        #    CopySource=event['Records'][0]['s3']['bucket']['name']+'/'+urllib.parse.unquote(event['Records'][0]['s3']['object']['key']),
        #    Key= 'config.yaml',
        #)
        #s3.delete_object(
        #    Bucket= event['Records'][0]['s3']['bucket']['name'],
        #    Key= urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
        #    )
        #file_path = move_directory(urllib.parse.unquote(event['Records'][0]['s3']['object']['key']), event['Records'][0]['s3']['bucket']['name'], 'work')
        #print(file_path)
        #redshift = boto3.client('redshift-data', 'eu-west-3')
        #response = redshift.execute_statement(
        #    ClusterIdentifier='taxicluster',
        #    Database='yellowtaxitrips',
        #    DbUser='ggripon',
        #    SecretArn='arn:aws:secretsmanager:eu-west-3:697616767308:secret:redshiftqueryeditor-ggripon-taxitrip_cluster_secret-OEJkI0',
        #    Sql="insert into staging_area.timedim values(1,1,1,1)",
        #    WithEvent=False
        #)
        #move_directory(file_path, event['Records'][0]['s3']['bucket']['name'], 'done')
        
    except Exception as e:
        #move_directory(file_path, event['Records'][0]['s3']['bucket']['name'], 'error')
        print(e)
        raise e
