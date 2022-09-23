"""
The worker that loads and reloads files form all .py files
and assemble them into a WF instance

Each file should have a yaml format doc containing
* require
* produce

There should be a task instance, either it's an instance of class
or a function.
"""

import argparse
import os.path
import sys
from importlib.machinery import SourceFileLoader
from typing import List

import yaml
from pydantic import BaseModel

from wf import Task, WF, WFServe
from wf.util.logger import get_logger

logger = get_logger(__name__)


class TaskDef(BaseModel):
    require: List[str]
    produce: List[str]


class WorkflowDef(BaseModel):
    require: List[str]
    output: List[str]
    produce: List[str] = None
    dump: List[str] | None = None


def load_workflow(path):
    path = os.path.abspath(path)
    files = os.listdir(path)
    wf_cfg = {}
    if 'workflow.yaml' in files:
        wf_cfg = yaml.safe_load(open(os.path.join(path, 'workflow.yaml')).read())
    else:
        return None
    logger.info(f'loading path {path}')
    pyfiles = list(filter(lambda x: x.endswith('.py'), files))
    sub_workflow = list(filter(lambda x: os.path.isdir(x), files))
    logger.info(sub_workflow)
    _, wf_name = os.path.split(path)
    # print(files)
    tasks = []
    for f in pyfiles:
        task_name = f[:-3]
        foo = SourceFileLoader(task_name, os.path.join(path, f)).load_module()
        if not hasattr(foo, 'task'):
            continue
        func = getattr(foo, 'task')
        if not callable(func):
            print(f'ERROR: task defined in {task_name} is not callable')
        task_spec = yaml.safe_load(foo.__doc__)
        task_spec = TaskDef.parse_obj(task_spec)

        logger.info(f'loading {task_name}')
        task = Task(
            task_name, func,
            require=task_spec.require,
            produce=task_spec.produce)
        tasks.append(task)
    # adding sub workflows
    for f in sub_workflow:
        sub_f = load_workflow(os.path.join(path, f))
        if sub_f is None:
            continue
        func, spec = sub_f
        task = Task(
            f, func,
            require=spec.require,
            produce=spec.produce
        )
        tasks.append(task)
    wf_cfg = WorkflowDef.parse_obj(wf_cfg)
    workflow = WF(wf_name, output=wf_cfg.output)
    workflow.add_task(tasks)
    return workflow, wf_cfg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start workers for DAG')

    parser.add_argument('path', type=str, help='path of your Workflow')
    args = parser.parse_args(sys.argv[1:])
    # print(args)
    w, _ = load_workflow(args.path)
    workflow_server = WFServe(w)
    workflow_server.serve()
