import unittest

from dingus import DingusTestCase, Dingus, exception_raiser

import httpqueue.model.queue as mod

class TestPriorityQueueInit(unittest.TestCase, DingusTestCase(mod.PriorityQueueDoc)):
    def setUp(self):
        self.connection = Dingus()
        self.priority = Dingus()
        self.task = Dingus()

    def test_init_registers_priority_doc(self):
        q = mod.PriorityQueue(self.connection, 'name')

        self.assertTrue(self.connection.calls('register', [mod.PriorityQueueDoc]))


class TestPriorityQueue(unittest.TestCase, DingusTestCase(mod.PriorityQueueDoc)):
    def setUp(self):
        self.connection = Dingus()
        self.priority = Dingus()
        self.task = Dingus()
        mod.ObjectId = Dingus()

        self.q = mod.PriorityQueue(self.connection, 'name')

    def test_push_creates_priority_doc(self):
        self.q.push(self.priority, self.task)

        self.assertTrue(self.q.collection.calls('PriorityQueueDoc'))
        doc = self.q.collection.PriorityQueueDoc()
        self.assertEqual(doc.priority, self.priority)
        self.assertEqual(doc.task, self.task)

        self.assertTrue(doc.calls('save'))

    def test_cant_cancel_nonexistant(self):
        self.q.collection.remove.return_value = {'n': 0}

        with self.assertRaises(KeyError):
            self.q.cancel(Dingus())

    def test_cancel_with_bad_id(self):
        mod.ObjectId = exception_raiser(mod.pymongo.errors.InvalidId)

        with self.assertRaises(mod.errors.InvalidId):
            self.q.cancel(Dingus())

    def test_cancel_succeeds(self):
        self.q.collection.remove.return_value = {'n': 1}
        self.q.cancel(Dingus())

        self.q.collection.calls('remove')

    def test_cant_ack_nonexistance(self):
        self.q.collection.remove.return_value = {'n': 0}

        with self.assertRaises(KeyError):
            self.q.ack(Dingus())

    def test_ack_with_bad_id(self):
        mod.ObjectId = exception_raiser(mod.pymongo.errors.InvalidId)

        with self.assertRaises(mod.errors.InvalidId):
            self.q.ack(Dingus())

    def test_ack_succeeds(self):
        self.q.collection.remove.return_value = {'n': 1}
        id = Dingus()
        self.q.ack(id)

        self.q.collection.calls('remove')

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
