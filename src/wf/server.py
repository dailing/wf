import json
import uuid

import redis
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from wf.api.worker import *
from wf.util.logger import get_logger

app = FastAPI()
logger = get_logger(__name__)

app.mount("/static", StaticFiles(directory="static"), name="static")
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)


@app.post('/api/worker/get_task')
def worker_get_task(req: REQ_worker_get_task):
    logger.info(req)
    r = redis.Redis(connection_pool=pool)
    res = r.rpop(req.task_name)
    if res is None:
        return []
    res = [json.loads(res)]
    logger.debug(res)
    return res


@app.post('/api/client/add_task')
def client_add_task(req: REQ_client_add_task):
    r = redis.Redis(connection_pool=pool)
    req_json = req.dict()
    task = dict(
        kwargs=req_json['kwargs'],
        task_id=uuid.uuid4().hex,
    )
    ret = r.lpush(req.task_name, json.dumps(task))
    logger.debug(ret)
