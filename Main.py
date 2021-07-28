import logging, pathlib
from FolderStructure import FolderStructure
from Workflow import Workflow

def lambda_handler(event, context):
    
    logging.basicConfig(format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s', level=logging.INFO)
    logging.info("Launching configuration procedures.")
    FileHandler = FolderStructure()
    Workflow(FileHandler, event['Records'][0]['s3']['bucket']['name'], event['Records'][0]['s3']['object']['key'])
    

if __name__ == "__main__":
# execute only if run as a script
    event = {
        "Records": [
            {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "eu-west-3",
            "eventTime": "1970-01-01T00:00:00.000Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "EXAMPLE"
            },
            "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
            },
            "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                "name": "postgretaxiconfig",
                "ownerIdentity": {
                    "principalId": "EXAMPLE"
                },
                "arn": "arn:aws:s3:::postgretaxiconfig"
                },
                "object": {
                "key": "config/config.yaml",
                "size": 1024,
                "eTag": "0123456789abcdef0123456789abcdef",
                "sequencer": "0A1B2C3D4E5F678901"
                }
            }
            }
        ]
        }

    lambda_handler(event, None)




