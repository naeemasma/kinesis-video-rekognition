import base64
import datetime
import json
import logging
import os

import boto3
import requests
from requests_aws4auth import AWS4Auth

logr = logging.getLogger(__name__)
logr.setLevel(logging.DEBUG)

# Fetch AWS OpenSearch service configuration from environmental variables
url_es = os.getenv("ES_URL")
region_aws = os.getenv("REGION")

# Get credentials
creds = boto3.Session().get_credentials()
aws_auth = AWS4Auth(creds.access_key, creds.secret_key, region_aws, 'es', session_token=creds.token)


def process_message(message):
    """ Send face recognition output to AWS OpenSearch service """
    msg_body = json.loads(base64.b64decode(message['kinesis']['data']))
    logr.info(f"record: {msg_body}")

    unix_time = msg_body["InputInformation"]["KinesisVideo"]["ServerTimestamp"]
    date_time = datetime.datetime.utcfromtimestamp(unix_time).strftime("%Y-%m-%dT%H:%M:%S+0000")

    for each_face in msg_body["FaceSearchResponse"]:
        if not each_face["MatchedFaces"]:
            continue
        match_confidence = each_face["MatchedFaces"][0]["Similarity"]
        match_name = each_face["MatchedFaces"][0]["Face"]["ExternalImageId"]

        msg = {"timestamp": date_time, "name": match_name, "confidence": match_confidence}
        resp = requests.post(f"https://{url_es}/face/_doc/",
                             auth=aws_auth,
                             headers={"Content-Type": "application/json"},
                             json=msg)
        logr.info(f"result: code={resp.status_code}, response={resp.text}")


def on_message(event, context):
    for message in event['Records']:
        process_message(message)
    return {"result": "ok"}
