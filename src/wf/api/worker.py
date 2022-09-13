from typing import Dict, Any

from pydantic import BaseModel


class REQ_worker_get_task(BaseModel):
    """
    get task parameters
    """
    task_name: str


class RESP_worker_get_task(BaseModel):
    kwargs: Dict[str, Any]


class REQ_client_add_task(BaseModel):
    task_name: str
    kwargs: Dict[str, Any]


class Msg(BaseModel):
    """
    get task parameters
    """
    status: str
    msg: str = ''
