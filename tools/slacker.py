#!/usr/bin/env python
import os
import json
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
credentials = os.path.join(BASE_DIR, 'credentials.json')
message = 'My first automated slack post'


def get_credentials(credentials=credentials):
    """
    Read credentials from JSON file.
    """
    with open(credentials, 'r') as f:
        creds = json.load(f)
    return creds['slack_webhook']


def post_to_slack(message=message, credentials=credentials):
    data = {'text': message}
    url = get_credentials(credentials)
    requests.post(url, json=data)


if __name__ == '__main__':
    post_to_slack(message, credentials)
