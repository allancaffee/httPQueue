"""
These tests verify the basic functionality of the system as a whole.
"""


import datetime

import mongokit

import httpqueue.app
from base_flask_test import BaseViewTest, TestingConfig


class TestAcceptance(BaseViewTest):
    "Test the app end to end."

    def setUp(self):
        BaseViewTest.setUp(self)
        self.epoch = datetime.datetime(1970, 1, 1, 0, 0, 0, 1).isoformat()
        self.nowish = datetime.datetime.utcnow().isoformat()

        conn = mongokit.Connection(TestingConfig.MONGODB_HOST,
                                   TestingConfig.MONGODB_PORT)
        conn.test.drop_collection('pq_foo')

    def _post_data(self, priority, data):
        resp = self.client.post('/queue/foo/',
                                headers=[('X-httPQueue-Priority', priority)],
                                data=data, content_type='application/json')

        self.assertEqual(resp.status_code, 200)

    def test_push_item(self):
        data = '{\n  "do": "something"\n}'
        self._post_data(self.nowish, data)

        resp = self.client.open(method='POP', path='/queue/foo/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data, data)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertIn('X-httPQueue-Priority', resp.headers)
        self.assertIn('X-httPQueue-ID', resp.headers)

        id = resp.headers['X-httPQueue-ID']
        resp = self.client.open(method='ACK', path='/queue/foo/id/%s' % id)

        self.assertEqual(resp.status_code, 200)

        # Nothing left.
        resp = self.client.open(method='POP', path='/queue/foo/')

        self.assertEqual(resp.status_code, 204)

    def test_returns_in_order(self):
        data_first = '{\n  "do": "first"\n}'
        data_second = '{\n  "do": "second"\n}'

        self._post_data(self.nowish, data_second)
        self._post_data(self.epoch, data_first)

        resp = self.client.open(method='POP', path='/queue/foo/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data, data_first)

        resp = self.client.open(method='POP', path='/queue/foo/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data, data_second)
