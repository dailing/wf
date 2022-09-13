from wf.tasks import *


def mul(a, b):
    return a * b


def add(a, b):
    return a + b


if __name__ == '__main__':
    tasks = [
        Task('add1', partial(add, 1), ['v:b'], ['t1']),
        Task('add5', partial(add, 5), ['t1:b'], ['t2']),
        Task('mul', mul, ['t1:a', 't2:b'], ['m'])
    ]

    wf = WF()
    wf.add_task(tasks)
    wf.graph()
    wf.serve()
    # pprint(wf.output_map)
    # pprint(wf.dependent_path('m'))
    print(wf.execute('m', v=1))
