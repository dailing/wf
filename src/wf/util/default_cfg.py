import requests
from pydantic import BaseModel

url_base = 'http://192.168.123.161:8000'


def post(url, data: BaseModel):
    url = f'{url_base}{url}'
    print(url)
    print(data.json())
    resp = requests.post(url, data=data.json())
    assert resp.status_code == 200
    return resp.json()
