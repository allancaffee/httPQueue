from dingus import DingusTestCase, Dingus, exception_raiser, DontCare
from nose.tools import assert_raises

from httpqueue.model.queue import PriorityQueue
import httpqueue.model.queue as mod

class WhenInitializingPriorityQueue(DingusTestCase(PriorityQueue)):
    def setup(self):
        super(WhenInitializingPriorityQueue, self).setup()
        self.connection = Dingus('connection')
        self.priority = Dingus('priority')
        self.task = Dingus('task')

        self.priority_queue = PriorityQueue(self.connection, 'name')

    def should_register_task_doc(self):
        assert self.connection.calls('register', [mod.TaskDoc]).once()


class DescribePriorityQueue(DingusTestCase(
    PriorityQueue, ['PRIORITY_QUEUE_PREFIX', 'PQInvalidId',
                    'PymongoInvalidId'])):

    def setup(self):
        super(DescribePriorityQueue, self).setup()
        self.connection = Dingus('connection')
        self.priority = Dingus('priority')
        self.task = Dingus('task')

        self.priority_queue = PriorityQueue(self.connection, 'name')


class WhenPushingPriorityDoc(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)

        self.priority_queue.push(self.priority, self.task)

    def should_create_task_doc_in_collection(self):
        assert self.priority_queue.collection.calls('TaskDoc').once()

    def should_set_priorty_on_task_document(self):
        doc = self.priority_queue.collection.TaskDoc()
        assert doc.priority == self.priority

    def should_set_task_body_as_task(self):
        doc = self.priority_queue.collection.TaskDoc()
        assert doc.task == self.task

    def should_save_task_doc(self):
        doc = self.priority_queue.collection.TaskDoc()
        assert doc.calls('save').once()


class WhenCancellingNonExistantTask(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)

        self.priority_queue.collection.remove.return_value = {'n': 0}

    def should_raise_key_error(self):
        assert_raises(KeyError, self.priority_queue.cancel, Dingus())


class WhenCancellingWithBadId(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)

        mod.ObjectId = exception_raiser(mod.PymongoInvalidId)

    def should_wrap_exception(self):
        assert_raises(mod.PQInvalidId,
                      self.priority_queue.cancel, Dingus())


class WhenCancellingTask(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)

        self.priority_queue.collection.remove.return_value = {'n': 1}
        self.priority_queue.cancel(Dingus())

    def should_remove_document(self):
        # TODO: Be way more specific...
        self.priority_queue.collection.calls('remove')


class WhenAckingNonExistantTask(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)

        self.priority_queue.collection.remove.return_value = {'n': 0}

    def should_raise_key_error(self):
        assert_raises(KeyError, self.priority_queue.ack, Dingus())


class WhenAckingWithBadId(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)
        mod.ObjectId = exception_raiser(mod.PymongoInvalidId)

    def should_raise_invalid_id_error(self):
        assert_raises(mod.PQInvalidId, self.priority_queue.ack, Dingus())


class WhenAckingSuceeds(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)
        self.priority_queue.collection.remove.return_value = {'n': 1}
        self.id = Dingus('id')
        self.priority_queue.ack(self.id)

    def should_remove_object_with_id(self):
        assert self.priority_queue.collection.calls(
            'remove', {'_id': mod.ObjectId(self.id),
                       'in_progress': True},
            safe=True)


class WhenPopping(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)
        # TODO: Don't mock privates
        self.priority_queue._calculate_expiration_time = Dingus()
        self.result = self.priority_queue.pop()

    def should_calculate_expiration_time(self):
        assert self.priority_queue._calculate_expiration_time.calls('()')

    def should_set_expire_time(self):
        expiration = self.priority_queue._calculate_expiration_time()
        assert self.priority_queue.collection.calls(
            'update', DontCare, {'$set': {'expire_time': expiration}})


class WhenListingQueues(DescribePriorityQueue):

    def setup(self):
        DescribePriorityQueue.setup(self)
        db = Dingus('db')
        db.collection_names.return_value = ['pq_empty', 'foo', 'pq_bar']
        self.result = mod.list_queues(db)

    def should_return_ones_starting_with_pq(self):
        assert self.result == ['empty', 'bar']

    def test_expiration_is_now_plus_delta(self):
        mod.datetime = Dingus()
        delta = Dingus()
        rv = self.q._calculate_expiration_time(delta)

        assert mod.datetime.datetime.calls('utcnow')
        assert mod.datetime.calls('timedelta', seconds=delta)
        self.assertEqual(rv, mod.datetime.datetime.utcnow() + mod.datetime.timedelta(delta))

    def test_list_queues_no_collections(self):
        db = Dingus()
        db.collection_names.return_value = []
        queues = mod.list_queues(db)

        self.assertEqual(queues, [])

    def test_restore_pending(self):
        mod.datetime = Dingus()
        self.q.restore_pending()

        assert self.q.collection.calls(
            'update', {'in_progress': True,
             'expire_time': {'$lte': mod.datetime.datetime.utcnow()}},
            {'$unset': {'expire_time': 1},
             '$set': {'in_progress': False}})
