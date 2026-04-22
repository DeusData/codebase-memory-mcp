import os
import requests


def fetch_me():
    return requests.get(os.getenv('USER_SERVICE_URL') + '/v1/me')


def submit_order():
    return requests.post('http://order-service/v1/orders', json={})
