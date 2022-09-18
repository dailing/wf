import uuid
from collections import namedtuple
from typing import List

import redis
from fastapi import FastAPI

from wf.api.worker import *
from wf.util.logger import get_logger

app = FastAPI()
logger = get_logger(__name__)

# app.mount("/static", StaticFiles(directory="static"), name="static")
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)

TASK_FIELD = namedtuple(
    'TASK_FIELD',
    [
        'queue',
        'status'
    ]
)


class TaskInfo:
    def __init__(self, task_name, redis_pool=None, task_id=None):
        self.name = task_name
        if redis_pool is None:
            redis_pool = pool
        self.pool = redis_pool
        self.task_id = task_id

    @property
    def conn(self):
        return redis.Redis(connection_pool=pool)

    @property
    def queue_name(self):
        return f'queue.{self.name}'

    @property
    def status_name(self):
        assert self.task_id is not None
        return f'status.{self.task_id}'

    def add_task(self, kwargs, output):
        if self.task_id is None:
            self.task_id = f'{self.name}.{uuid.uuid4().hex}'
        task = TaskBase(task_name=self.name, kwargs=kwargs, output=output, task_id=self.task_id)
        ret = self.conn.lpush(self.queue_name, task.json())
        return self.task_id

    def pop_task(self):
        res = self.conn.rpop(self.queue_name)
        if res is None:
            return []
        task = RESP_worker_get_task.parse_raw(res)
        return [task]


@app.post('/api/worker/get_task')
def worker_get_task(req: REQ_worker_get_task) -> List[RESP_worker_get_task]:
    logger.debug(f'get task {req}')
    res = TaskInfo(req.task_name).pop_task()
    logger.debug(res)
    return res


@app.post('/api/client/add_task')
def client_add_task(req: REQ_client_add_task) -> RESP_client_add_task:
    logger.debug(f'add_task {req}')
    t = TaskInfo(req.task_name)
    tid = t.add_task(req.kwargs, req.output)
    return RESP_client_add_task(task_id=tid)


@app.post('/api/worker/add_result')
def worker_add_result():
    pass