from typing import Dict, Any, List

from pydantic import BaseModel


class REQ_worker_get_task(BaseModel):
    """
    get task parameters
    """
    task_name: str


class TaskBase(BaseModel):
    task_name: str
    kwargs: Dict[str, Any]
    output: List[str]
    loop: int = 1
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
