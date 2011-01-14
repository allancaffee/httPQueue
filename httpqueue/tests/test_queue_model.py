import unittest
import datetime

from mongokit.schema_document import RequireFieldError
from mongokit import OperationFailure, Connection
from dingus import Dingus

import httpqueue.model.queue as mod

class TestPriorityQueue(unittest.TestCase):
    def setUp(self):
        self.connection = Connection('localhost', 27017)
        for c in self.connection.test.collection_names():
            if c.startswith('pq_'):
                self.connection.test.drop_collection(c)

        self.empty_q = mod.PriorityQueue(self.connection, 'empty')
        self.empty_q.collection.remove()
        self.epoch = datetime.datetime(1970, 1, 1, 0, 0, 0)
        self.nowish = datetime.datetime.utcnow()

    def test_cant_insert_none(self):
        with self.assertRaises(RequireFieldError):
            self.empty_q.push(self.epoch, None)

    def test_cant_have_priority_none(self):
        with self.assertRaises(RequireFieldError):
            self.empty_q.push(None, u'foo')

    def test_cant_pop_empty_queue(self):
        with self.assertRaises(OperationFailure):
            self.empty_q.pop()

    def test_returns_pushed_item(self):
        q = self.empty_q
        q.push(self.epoch, u'foo')
        result = q.pop()

        self.assertEqual(result['task'],  'foo')

    def test_returns_by_priority(self):
        q = self.empty_q
        q.push(self.nowish, u'foo')
        q.push(self.epoch, u'bar')
        first = q.pop()
        second = q.pop()

        self.assertEqual(first['task'], 'bar')
        self.assertEqual(second['task'], 'foo')

    def test_cant_cancel_nonexistant(self):
        with self.assertRaises(KeyError):
            self.empty_q.cancel('unknown')

    def test_cancel_removes_from_queue(self):
        q = self.empty_q
        id = q.push(self.epoch, u'foo')
        q.cancel(id)

        with self.assertRaises(OperationFailure):
            q.pop()

    def test_list_queues(self):
        db = Dingus()
        db.collection_names.return_value = ['pq_empty', 'bar']
        queues = mod.list_queues(db)

        self.assertEqual(queues, [u'empty'])

    def test_list_queues_only_includes_queues(self):
        db = Dingus()
        db.collection_names.return_value = ['pq_empty', 'foo', 'pq_bar']
        queues = mod.list_queues(db)

        self.assertEqual(queues, ['empty', 'bar'])

    def test_list_queues_no_collections(self):
        db = Dingus()
        db.collection_names.return_value = []
        queues = mod.list_queues(db)

        self.assertEqual(queues, [])
