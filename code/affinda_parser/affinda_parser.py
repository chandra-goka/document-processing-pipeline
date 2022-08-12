import json
import os
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

DOCUMENT_REGISTRY_TABLE = os.environ.get('DOCUMENT_REGISTRY_TABLE')

def read_raw_file(bucket_name, document_name):
    # Create resume with file
    print(f"reading raw file from {bucket_name} - {document_name}")
    S3Helper().downloadFile(bucket_name, document_name, f'/tmp/{document_name}')
    file_pth = Path(f'/tmp/{document_name}')
    with open(file_pth, "rb") as f:
        resume = client.create_resume(file=f)
        return resume.as_dict()


def upload_algolia_search(result, document_id):
    result['objectID'] = document_id
    print(f"uploading results to algolia :: {result}")
    res = algolia_index.save_objects([result])
    print("upload_algolia_search completed..")


def get_file_name(path):
    file_name = path.rsplit('/', 1)[1]
    document_id = path.rsplit('/', -1)[0]
    return file_name, document_id


def get_raw_file_details(document_id):
    print("getting raw file..")
    data = DynamoDBHelper().getItems(DOCUMENT_REGISTRY_TABLE, 'documentId', document_id)
    record = data[0]
    document_name = record.get("documentName")
    bucket_name = record.get("bucketName")
    return bucket_name, document_name


def processRequest(bucketName, documentName):
    print("documentName :: ",documentName)
    file_name, document_id = get_file_name(documentName)
    S3Helper().downloadFile(bucketName, documentName, f'/tmp/{file_name}')
    file_pth = Path(f'/tmp/{file_name}')
    with open(file_pth, "rb") as f:
        resume = client.create_resume(file=f)
        r_dict = resume.as_dict()
        print("document_id :: ",r_dict.get("documentId"))
    if resume:
        resume_data = resume.as_dict()
        bucket_name, document_name = get_raw_file_details(document_id)
        raw_file_data = read_raw_file(bucket_name, document_name)
        resume_data.update(raw_file_data)
        upload_algolia_search(resume_data, document_id)
    else:
        print("No data from affinda")


def lambda_handler(event, context):
    print("event: {}".format(event))
    for record in event['Records']:
        if 'eventSource' in record and record['eventSource'] == 'aws:s3':
            bucketName = record['s3']['bucket']['name']
            documentName = urllib.parse.unquote_plus(record['s3']['object']['key'])
            documentVersion = record['s3']['object'].get('versionId', None)
            principalIAMWriter = record['userIdentity']['principalId']
            eventName = record['eventName']
            if eventName.startswith("ObjectCreated"):
                processRequest(bucketName, documentName)
            else:
                print("Processing not yet implemented")
        else:
            print("Uninvoked recorded event structure.")