import json
import os
import uuid
import urllib.parse
from metadata import PipelineOperationsClient
from helper import FileHelper, S3Helper, DynamoDBHelper
from affinda import AffindaAPI, TokenCredential
from algoliasearch.search_client import SearchClient
from pathlib import Path


# Affinda Properties
token              = os.environ.get('AFFINDA_TOKEN')
credential         = TokenCredential(token=token)
credential = TokenCredential(token=token)
client = AffindaAPI(credential=credential)


# Algolia Properties
ALGOLIA_APP_ID = os.environ.get('ALGOLIA_APP_ID')
ALGOLIA_API_KEY = os.environ.get('ALGOLIA_API_KEY')
ALGOLIA_INDEX_NAME = os.environ.get('ALGOLIA_INDEX_NAME')
algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_API_KEY)
algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME)


def upload_to_affinda(newImage):
    print("uploading to affinda api")
    bucketName = newImage.get("bucketName")
    objectName = newImage.get("documentName")
    # Create resume with file
    S3Helper().downloadFile(bucketName, objectName, f'/tmp/{objectName}')
    file_pth = Path(f'/tmp/{objectName}')
    with open(file_pth, "rb") as f:
        resume = client.create_resume(file=f)
        print(resume.as_dict())
        return resume.as_dict()


def upload_algolia_search(result):
    result['objectID'] = str(uuid.uuid1())
    print(f"uploading results to algolia :: {result}")
    res = algolia_index.save_objects([result])
    print("upload_algolia_search completed..")

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
    result = upload_to_affinda(invokedItem)
    upload_algolia_search(result)