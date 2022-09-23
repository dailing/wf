from wf.client import WorkerFlowClient

if __name__ == '__main__':
    client = WorkerFlowClient(name='basic', output=['c'])
    result = client(a=1, b=2)
    print(result.get())
