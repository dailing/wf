import uuid
from collections import namedtuple

import redis
from fastapi import FastAPI
from pydantic import parse_obj_as
from pymongo import MongoClient

from wf.api.worker import *
from wf.util.logger import get_logger

app = FastAPI()
logger = get_logger(__name__)

# app.mount("/static", StaticFiles(directory="static"), name="static")
pool = redis.ConnectionPool(host='redis', port=6379, db=0)

mongo_client = MongoClient('mongodb://root:example@mongodb:27017')
result_collection = mongo_client['ResultDB']['task_result']

TASK_FIELD = namedtuple(
    'TASK_FIELD',
    [
        'queue',
        'status'
    ]
)


class TaskInfo:
    def __init__(self, task_name=None, redis_pool=None, task_id=None):
        self.name = task_name
        if redis_pool is None:
            redis_pool = pool
        self.pool = redis_pool
        self.task_id = task_id
        self.mongo_collection = result_collection

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

    @property
    def lock_name(self):
        assert self.task_id is not None
        return f'lock.{self.task_id}'

    def add_task(self, kwargs, output):
        if self.task_id is None:
            self.task_id = f'{self.name}.{uuid.uuid4().hex}'
        task = TaskBase(task_name=self.name, kwargs=kwargs, output=output, task_id=self.task_id)
        # setup status of the task
        assert self.conn.exists(self.status_name) == 0, f'Task inited twice'
        self.conn.set(self.status_name, TaskStatus.RUNNING)

        # set up result waiting lock for this task
        self.conn.lpush(self.lock_name, 'lock')

        # add task to task queue
        _list_len = self.conn.lpush(self.queue_name, task.json())
        assert _list_len > 0
        return self.task_id

    def pop_task(self):
        res = self.conn.rpop(self.queue_name)
        if res is None:
            return []
        task = RESP_worker_get_task.parse_raw(res)
        return [task]

    def add_result(self, req: REQ_worker_add_result):
        assert self.task_id is not None
        result_collection.insert_one(req.result.dict())
        status = self.conn.getset(self.status_name, req.result.status)
        assert status is not None
        status = status.decode('utf-8')
        assert status == TaskStatus.RUNNING, \
            f'Trying to add result, however status of task is {status}, {type(status)}'
        # ending the task, clear keys in redis
        if req.result.status != TaskStatus.RUNNING:
            assert self.conn.expire(self.status_name, 10) == 1, 'error setting expiration'
            p = self.conn.pipeline()
            p.lpush(self.lock_name, 'lock')
            p.expire(self.lock_name, 20)
            p.execute()

    def get_result(self, index=None, wait=False):
        if wait:
            # ASSUME THAT ONLY ONE CLIENT WAIT FOR TASK
            ttl = self.conn.ttl(self.lock_name)
            if ttl == -1:
                # key exist and ttl not set, So result exists but not ready,
                # waiting for tasks
                self.conn.brpop(self.lock_name)
                self.conn.brpop(self.lock_name)
            # if ttl == -2, key not exists, so the task is finished
            # if ttl >=0, task finished and ttl already set
        if index is None:
            return list(self.mongo_collection.aggregate([
                {
                    '$match': {
                        'task_id': self.task_id
                    }
                }
            ]))
        return next(self.mongo_collection.find({
            'task_id': self.task_id,
            'n_iter': index,
        }))


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
def worker_add_result(req: REQ_worker_add_result):
    logger.debug(f'add result {req}')
    task = TaskInfo(task_id=req.result.task_id)
    task.add_result(req)


@app.post('/api/worker/get_result')
def worker_add_result(req: REQ_client_get_result) -> List[RESP_client_get_result]:
    task = TaskInfo(task_id=req.task_id)
    result = task.get_result(req.index, req.wait)
    return parse_obj_as(List[RESP_client_get_result], result)
