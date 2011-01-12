import unittest

from dingus import Dingus, exception_raiser

import httpqueue.app
import httpqueue.views.queue as mod

class TestingConfig(object):
    MONGODB_HOST = 'localhost'
    MONGODB_PORT = 27017

class BaseViewTest(unittest.TestCase):
    def setUp(self):
        self.app = httpqueue.app.make_app(TestingConfig)
        self.client = self.app.test_client()

class TestQueueView(BaseViewTest):
    def setUp(self):
        BaseViewTest.setUp(self)
        mod.model = Dingus()

    def test_push_item_adds_to_queue(self):
        resp = self.client.post('/queue/foo/')

        assert mod.model.calls('get_queue', 'foo')

    def test_ack_item_succeeds(self):
        resp = self.client.delete('/queue/foo/', headers=[('X-httPQueue-ID', '1')])

        assert mod.model.get_queue('foo').calls('ack', '1')
        self.assertEqual(resp.status_code, 200)

    def test_ack_nonexistant_returns_404(self):
        mod.model.get_queue().ack = exception_raiser(KeyError)
        resp = self.client.delete('/queue/foo/', headers=[('X-httPQueue-ID', '1')])

        self.assertEqual(resp.status_code, 404)

    def test_list_queues_returns_json(self):
        mod.model.queue.list_queues.return_value = ['pq_foo', 'pq_bar']
        resp = self.client.get('/queue/')

        self.assertEqual(resp.data, '["pq_foo", "pq_bar"]')
        self.assertEqual(resp.status_code, 200)
