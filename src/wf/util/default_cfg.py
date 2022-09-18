import requests
from pydantic import BaseModel, parse_obj_as
from .logger import get_logger

logger = get_logger(__name__)

url_base = 'http://192.168.123.161:8000'


# url_base = 'http://127.0.0.1:8000'


def post(url, data: BaseModel, resp_type=None):
    url = f'{url_base}{url}'
    logger.debug(f'requesting {url}')
    # print(data.json())
    resp = requests.post(url, data=data.json())
    assert resp.status_code == 200
    if resp_type is None:
        return resp.json()
    else:
        try:
            return parse_obj_as(resp_type, resp.json())
        except TypeError:
            return resp.json()
