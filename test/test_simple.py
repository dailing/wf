import os.path
import unittest

from wf.client import WorkerFlowClient

from wf import WFServe

from wfworker import load_workflow


class TestAddingSimpleCase(unittest.TestCase):
    def setUp(self) -> None:
        path, _ = os.path.split(__file__)
        w, _ = load_workflow(os.path.join(path, 'test_basic'))
        self.workflow_server = WFServe(w)
        self.client = WorkerFlowClient(name='test_basic', output=['c'])

    def test_get_result(self):
        result = self.client(a=1, b=2, loop=2)
        self.workflow_server.serve(1)
        rr = result.get()
        self.assertIn('c', rr)
        self.assertEqual(rr['c'], 6)