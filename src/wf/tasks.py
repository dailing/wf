import hashlib
import inspect
import pickle
import types
from functools import cached_property, partial
from pprint import pprint
from typing import Dict, List, Iterable

import redis


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
        if not isinstance(result, Iterable):
            result = (result,)
        if self.produce is None:
            return None
        r_dict = {}
        for k, v in zip(self.produce, result):
            if k == '_':
                pass
            r_dict[k] = v
        return r_dict


def __repr__(self) -> str:
    return f'Task <{self.name}> {self.require}-->{self.produce}'


class WF:
    def __init__(self):
        self.tasks: List[Task] = []

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

    def dependent_path(self, output) -> List[Task]:
        if output not in self.output_map:
            return []
        task = self.output_map[output]
        dependency = []
        for req in task.require:
            for partial_dep in self.dependent_path(req):
                if partial_dep not in dependency:
                    dependency.append(partial_dep)
        dependency.append(task)
        return dependency

    def execute(self, output=None, **kwargs):
        data = kwargs
        exec_list = self.dependent_path(output)
        for task in exec_list:
            task.load()
            print('executing ', task)
            exec_kwargs = {}
            for d, k in task.require_map.items():
                exec_kwargs[k] = data[d]
            print(task, exec_kwargs)
            res = task(**exec_kwargs)
            data.update(res)
            pprint(res)
        if isinstance(output, str):
            return data[output]
        if isinstance(output, list) or isinstance(output, tuple):
            return {k: data[k] for k in output}
        raise Exception('FUCK')

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

    def serve(self):
        r = redis.Redis(host='localhost', port=6379, db=0)
