from dingus import Dingus, exception_raiser

import httpqueue.views.queue as mod
from base_flask_test import BaseViewTest


class TestQueueView(BaseViewTest):
    def setUp(self):
        BaseViewTest.setUp(self)
        # Don't mock out the error module.
        errors = mod.model.errors
        mod.model = Dingus()
        mod.model.errors = errors

    def test_push_without_priority_returns_400(self):
        resp = self.client.post('/queue/foo/')

        self.assertEqual(resp.status_code, 400)

    def test_push_with_bad_priority_returns_400(self):
        resp = self.client.post('/queue/foo/', headers=[('X-httPQueue-Priority', 'not-a-valid-date')])

        self.assertEqual(resp.status_code, 400)

    def test_push_item_adds_to_queue(self):
        resp = self.client.post('/queue/foo/', headers=[('X-httPQueue-Priority', '2011-01-13T02:00:51.577113')])

        assert mod.model.calls('get_queue', 'foo')

    def test_rejects_bad_json(self):
        mod.json = Dingus()
        mod.json.loads = exception_raiser(ValueError)
        resp = self.client.post('/queue/foo/', headers=[('X-httPQueue-Priority', '2011-01-13T02:00:51.577113')],
                                content_type='application/json')

        self.assertEqual(resp.status_code, 415)

    def test_ack_item_succeeds(self):
        resp = self.client.open(method='ACK', path='/queue/foo/id/1')

        self.assertEqual(resp.status_code, 200)
        assert mod.model.get_queue('foo').calls('ack', '1')

    def test_ack_nonexistant_returns_404(self):
        mod.model.get_queue().ack = exception_raiser(KeyError)
        resp = self.client.open(method='ACK', path='/queue/foo/id/1')

        self.assertEqual(resp.status_code, 404)

    def test_ack_invalid_id_returns_404(self):
        mod.model.get_queue().ack = exception_raiser(mod.model.errors.InvalidId)
        resp = self.client.open(method='ACK', path='/queue/foo/id/NotValid')

        self.assertEqual(resp.status_code, 404)

    def test_list_queues_returns_json(self):
        mod.model.queue.list_queues.return_value = ['pq_foo', 'pq_bar']
        resp = self.client.get('/queue/')

        self.assertEqual(resp.data, '["pq_foo", "pq_bar"]')
        self.assertEqual(resp.status_code, 200)

    def test_pop_from_empty_returns_204(self):
        mod.model.get_queue().pop.return_value = None
        resp = self.client.open(method='POP', path='/queue/foo/')

        self.assertEqual(resp.status_code, 204)

    def test_pop_restores_pending(self):
        self.client.open(method='POP', path='/queue/foo/')

        assert mod.model.get_queue().calls('restore_pending')
