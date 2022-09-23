import base64
import hashlib
import inspect
import pickle
import time
import types
import uuid
from functools import cached_property, partial, lru_cache

from .api.worker import *
from .util.default_cfg import post
from .util.logger import get_logger

logger = get_logger(__name__)


class Task:
    def __init__(self, name, func: callable, require=None, produce=None) -> None:
        super().__init__()
        self.func = func
        self.produce = produce
        self.name = name
        self.require_map = {}
        if require is None:
            require = []
        for req in require:
            req_split = req.split(':')
            self.require_map[req_split[0]] = req_split[-1]
        self.require = list(self.require_map.keys())
        self.loaded = False

    @cached_property
    def func_hash(self):
        code = self.func_code
        return hashlib.md5(code).hexdigest()

    @cached_property
    def func_code(self):
        if isinstance(self.func, partial):
            return pickle.dumps(self.func)
        elif isinstance(self.func, types.FunctionType):
            return inspect.getsource(self.func).encode('utf-8')
        else:
            return inspect.getsource(self.__class__)

    def load(self):
        if self.loaded:
            return
        if hasattr(self.func, 'load'):
            self.func.load()
        self.loaded = True

    def unload(self):
        """
        release resources
        used for long time idle
        """
        if not self.loaded:
            return
        if hasattr(self.func, 'unload'):
            self.func.unload()
        self.unload()

    def __call__(self, **kwargs):
        result = self.func(**kwargs)
        if self.produce is None:
            return None
        if len(self.produce) == 1:
            result = (result,)
        r_dict = {}
        for k, v in zip(self.produce, result):
            if k == '_':
                pass
            r_dict[k] = v
        return r_dict

    def __repr__(self):
        return f'<{self.name}>'


def __repr__(self) -> str:
    return f'Task <{self.name}> {self.require}-->{self.produce}'


class WF:
    def __init__(self, name=None, output=None, require=None):
        self.tasks: List[Task] = []
        if name is None:
            name = 'unnamed-' + uuid.uuid4().hex
        self.name = name
        self.output = output
        self.require = require

    def add_task(self, task):
        if isinstance(task, Task):
            self.tasks.append(task)
        else:
            self.tasks.extend(task)
        for t in self.tasks:
            print(t.func_hash)

    @property
    def output_map(self) -> Dict[str, Task]:
        produce = {}
        for t in self.tasks:
            for o in t.produce:
                produce[o] = t
        return produce

    @lru_cache
    def dependent_path(self, _output) -> List[Task]:
        logger.info(f'find dependent path for {_output}')
        dependency = []
        for output in _output:
            if output not in self.output_map:
                return []
            task = self.output_map[output]
            for req in task.require:
                for partial_dep in self.dependent_path((req,)):
                    if partial_dep not in dependency:
                        dependency.append(partial_dep)
            dependency.append(task)
        return dependency

    def execute(self, output, loop=1, **kwargs):
        logger.debug(f'executing workflow <{self.name}> {output} {type(output)}')
        exec_list = self.dependent_path(tuple(output))
        for iter_loop in range(loop):
            context = kwargs
            try:
                for task in exec_list:
                    logger.info(f'executing {task}')
                    task.load()
                    # print('executing ', task)
                    exec_kwargs = {}
                    for d, k in task.require_map.items():
                        exec_kwargs[k] = context[d]
                    # print(task, exec_kwargs)
                    res = task(**exec_kwargs)
                    logger.info(f'result {res}')
                    context.update(res)
                    # pprint(res)
                # if isinstance(output, str):
                #     return data[output]
                # if isinstance(output, list) or isinstance(output, tuple):
            except BreakLoopException:
                break
            finally:
                yield {k: context[k] for k in output}

    def __call__(self, **kwargs):
        res = next(self.execute(output=self.output, **kwargs))
        res = tuple([res[o] for o in self.output])
        if len(self.output) == 1:
            return res[0]
        return res

    def graph(self):
        import graphviz
        dot = graphviz.Digraph(comment='The Round Table')
        output_req = set()
        for t in self.tasks:
            output_req.update(t.produce)
            output_req.update(t.require)
            dot.node(t.name, f'{t.name}', shape='box', style='filled', color='lightgrey')
        for n in output_req:
            dot.node(n, f'{n}')
        for t in self.tasks:
            for req in t.require:
                dot.edge(req, t.name)
            for output in t.produce:
                dot.edge(t.name, output)
        dot.render('output', format='pdf')


class WFServe:
    def __init__(self, wf: WF):
        self.wf = wf

    def serve(self, n=-1):
        while True:
            logger.info(n)
            if n == 0:
                break
            elif n > 0:
                n = n - 1
            try:
                req = REQ_worker_get_task(task_name=self.wf.name)
                resp: List[RESP_worker_get_task] = \
                    post('/api/worker/get_task', req, List[RESP_worker_get_task])
                if len(resp) <= 0:
                    time.sleep(3)
                for task in resp:
                    task: RESP_worker_get_task
                    result = self.wf.execute(
                        task.output,
                        **task.kwargs
                    )
                    for idx, r in enumerate(result):
                        logger.debug(f'{idx}, {r}')
                        post('/api/worker/add_result', REQ_worker_add_result(
                            result=TaskResult(
                                result_type='pickle',
                                payload=base64.b64encode(pickle.dumps(r)),
                                task_id=task.task_id,
                                n_iter=idx,
                                status=TaskStatus.RUNNING
                            ),
                        ))
                    # finished work,send FINISH status
                    post('/api/worker/add_result', REQ_worker_add_result(
                        result=TaskResult(
                            result_type='EOT',
                            task_id=task.task_id,
                            n_iter=-1,
                            status=TaskStatus.FINISHED
                        ),
                    ))
            except Exception as e:
                print(e)
                time.sleep(3)
                raise e
