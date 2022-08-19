import json
import os
import urllib.parse
from metadata import PipelineOperationsClient
from helper import FileHelper, S3Helper, DynamoDBHelper
from affinda import AffindaAPI, TokenCredential
from algoliasearch.search_client import SearchClient
from pathlib import Path
import requests
import json
import uuid
import time
from random import randint, randrange

# Affinda Properties
token              = os.environ.get('AFFINDA_TOKEN')
credential         = TokenCredential(token=token)
credential = TokenCredential(token=token)
client = AffindaAPI(credential=credential)

# Humantic AI Properties
HUMANTIC_AI_KEY = os.environ.get('HUMANTIC_AI_KEY')

# Algolia Properties
ALGOLIA_APP_ID = os.environ.get('ALGOLIA_APP_ID')
ALGOLIA_API_KEY = os.environ.get('ALGOLIA_API_KEY')
ALGOLIA_INDEX_NAME = os.environ.get('ALGOLIA_INDEX_NAME')
algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_API_KEY)
algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME)

DOCUMENT_REGISTRY_TABLE = os.environ.get('DOCUMENT_REGISTRY_TABLE')


def get_tmp_file_path(bucket_name, document_name):
    S3Helper().downloadFile(bucket_name, document_name, f'/tmp/{document_name}')
    return Path(f'/tmp/{document_name}')

def create_algolia_resume_raw_file(bucket_name, document_name):
    # Create resume with file
    print(f"reading raw file from {bucket_name} - {document_name}")
    file_pth = get_tmp_file_path(bucket_name, document_name)
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


def create_humantic_user_profile(file_pth, file_name):
    BASE_URL = "https://api.humantic.ai/v1/user-profile/create"  # Base URL for the CREATE endpoint
    headers = {}
    USER_ID = f"test{str(randrange(100, 100000))}"
    # Analysis ID: required; User profile link from LinkedIn or, User Email ID
    # or, for document or text, use any unique identifier. We suggest using a value that helps you identify the analysis easily.
    USER_ID = "test12345678"  # or, any unique identifier

    url = f"{BASE_URL}?apikey={HUMANTIC_AI_KEY}&userid={USER_ID}"

    # if not the case of text based input
    payload = {}

    # Document: required; only PDF and DOCX format of document files are supported.
    # key name must be "document" and value must be a file
    files = [
      ("document", (os.path.basename(file_pth), open(file_pth, "rb"), "application/octet-stream"))
    ]

    response = requests.request("POST", url, data=payload, headers=headers, files=files)

    print(response.status_code, response.text)
    print("humantic_user_profile response : ", response.text)
    if response.status_code == 200:
        create_resp = json.loads(response.text)
        user_id = create_resp['results']['userid']
        return user_id
    return False


def fetch_humantic_user_profile(user_id):
    PERSONA = "sales"
    time.sleep(15)
    url = f"https://api.humantic.ai/v1/user-profile?apikey={HUMANTIC_AI_KEY}&id={user_id}&persona={PERSONA}"
    headers = {
      'Content-Type': 'application/json'
    }
    print(f"Getting user profile user_id - {user_id} \n apiKey - {HUMANTIC_AI_KEY} \n url - {url}")

    response = requests.request("GET", url, headers=headers)
    print(f"user response = {response.text}")
    print(f"user response  status_code = {response.status_code}")
    if response.status_code == 200:
        user_profile = json.loads(response.text)
        user_result = user_profile.get("results")
        return user_result
    return {}


def get_humantic_data(bucket_name, document_name, file_name):
    file_pth = get_tmp_file_path(bucket_name, document_name)
    user_id = create_humantic_user_profile(file_pth, file_name)
    print(f"humantic user_id : {user_id}")
    if user_id:
        return fetch_humantic_user_profile(user_id)
    return {}

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
        raw_file_data = create_algolia_resume_raw_file(bucket_name, document_name)
        if raw_file_data:
            print("raw profile algolia response received")
            resume_data['raw_file_data'] = raw_file_data
        humantic_details = get_humantic_data(bucket_name, document_name, file_name)
        resume_data['humantic_ai'] = humantic_details
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