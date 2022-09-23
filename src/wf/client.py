import base64
import pickle
from typing import List

from pydantic import parse_obj_as

from wf import REQ_client_add_task, post, RESP_client_add_task, REQ_client_get_result, RESP_client_get_result


class WfResult:
    def __init__(self, task_id):
        self.task_id = task_id
        # if the task is finished on the server side, it is set to True
        self._ready = False
        # if the task is ready and the result is retrieved, it is set to True
        self._valid = False
        self._raw = []
        self._last_index = -1

    @property
    def ready(self):
        if self._ready:
            return True
        self._get_result(wait=False)
        return self._ready

    def get(self):
        if self.ready:
            return self._raw[self._last_index]
        self._get_result(wait=True)
        return self._raw[self._last_index]

    # def __getitem__(self, index):
    #     return self._raw[index]

    def _get_result(self, wait=False, index=None):
        """
        check the status of result
        :param wait: whether to wait for the result
        :return: None
        """
        res = \
            post('/api/worker/get_result', REQ_client_get_result(
                task_id=self.task_id,
                index=index,
                wait=wait,
            ))
        res = parse_obj_as(List[RESP_client_get_result], res)
        self._raw = []
        for r in res[:-1]:
            self._raw.append(pickle.loads(base64.b64decode(r.payload)))

    def __repr__(self):
        return f'<Result of Task={self.task_id}>'


class WorkerFlowClient:
    def __init__(
            self,
            url='http://localhost:8000',
            name=None,
            output=None
    ):
        self.url = url
        self.name = name
        self.output = output

    def __call__(self, **kwargs):
        payload = REQ_client_add_task(
            task_name=self.name,
            kwargs=kwargs,
            output=self.output,
        )
        resp: RESP_client_add_task \
            = post('/api/client/add_task', payload, RESP_client_add_task)
        # print(resp)
        return WfResult(resp.task_id)
