from enum import Enum
from typing import Dict, Any, List

from pydantic import BaseModel


class TaskStatus(str, Enum):
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    EXCEPTION = "EXCEPTION"


class REQ_worker_get_task(BaseModel):
    """
    get task parameters
    """
    task_name: str


class TaskBase(BaseModel):
    task_name: str
    kwargs: Dict[str, Any]
    output: List[str]
    task_id: str = ''


class RESP_worker_get_task(TaskBase):
    pass


class REQ_client_add_task(TaskBase):
    pass


class RESP_client_add_task(BaseModel):
    task_id: str


class Msg(BaseModel):
    """
    get task parameters
    """
    status: str
    msg: str = ''


class TaskResult(BaseModel):
    result_type: str
    payload: bytes | str | None = None
    task_id: str
    n_iter: int
    status: TaskStatus


class REQ_worker_add_result(BaseModel):
    result: TaskResult


class REQ_client_get_result(BaseModel):
    task_id: str
    index: int | None = None
    wait: bool = False


class RESP_client_get_result(TaskResult):
    pass


class BreakLoopException(Exception):
    pass
