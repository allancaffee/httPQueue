"""
These tests verify the basic functionality of the system as a whole.
"""


import datetime

import httpqueue.app
from base_flask_test import BaseViewTest


class TestAcceptance(BaseViewTest):
    "Test the app end to end."

    def test_push_item(self):
        data = '{\n  "do": "something"\n}'
        priority = datetime.datetime.utcnow().isoformat()
        resp = self.client.post('/queue/foo/',
                                headers=[('X-httPQueue-Priority', priority)],
                                data=data, content_type='application/json')

        self.assertEqual(resp.status_code, 200)

        resp = self.client.get('/queue/foo/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data, data)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertIn('X-httPQueue-Priority', resp.headers)
        self.assertIn('X-httPQueue-ID', resp.headers)

        id = resp.headers['X-httPQueue-ID']
        resp = self.client.delete('/queue/foo/',
                                  headers=[('X-httPQueue-ID', id)])

        self.assertEqual(resp.status_code, 200)

        resp = self.client.get('/queue/foo/')

        self.assertEqual(resp.status_code, 204)
