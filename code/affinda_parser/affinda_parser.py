import json
import os
import uuid
import urllib.parse
from metadata import PipelineOperationsClient
from helper import FileHelper, S3Helper, DynamoDBHelper
from affinda import AffindaAPI, TokenCredential
from pathlib import Path


token              = os.environ.get('AFFINDA_TOKEN', "d6d22208c807d204549c7c4b6a13c4b210d04ebf")
credential         = TokenCredential(token=token)
credential = TokenCredential(token=token)
client = AffindaAPI(credential=credential)

def upload_to_affinda(newImage):
    print("in uploadToAffinda >>")
    bucketName = newImage.get("bucketName")
    objectName = newImage.get("documentName")
    # Create resume with file
    S3Helper().downloadFile(bucketName, objectName, f'/tmp/{objectName}')
    file_pth = Path(f'/tmp/{objectName}')
    with open(file_pth, "rb") as f:
        resume = client.create_resume(file=f)
        print(resume.as_dict())


def lambda_handler(event, context):
    print(f"event: {event}")
    if "Records" in event and event["Records"]:
        for record in event["Records"]:
            try:
                if "eventName" in record and record["eventName"] in ["INSERT", "MODIFY"]:
                    if "dynamodb" in record and record["dynamodb"] and "NewImage" in record["dynamodb"]:
                        print("Processing record: {}".format(record))
                        invokedItem = DynamoDBHelper.deserializeItem(record["dynamodb"]["NewImage"])
                else:
                    print("Record not an INSERT or MODIFY event in DynamoDB")
            except Exception as e:
                print("Failed to process record. Exception: {}".format(e))
    upload_to_affinda(invokedItem)