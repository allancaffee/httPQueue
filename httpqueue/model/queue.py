import datetime

from mongokit import Document, Connection, ObjectId, OperationFailure
import pymongo.errors

import errors

# The prefix added to collections that belong to priority queues.
PRIORITY_QUEUE_PREFIX = 'pq_'

class PriorityQueueDoc(Document):
    structure = {
        'priority': datetime.datetime,
        'task': None,
        'in_progress': bool,
        'expire_time': datetime.datetime,
        'pending_life': int,
    }

    required_fields = ['priority', 'task']
    default_values = {'in_progress': False, 'pending_life': 5 * 60}

    indexes = [
        {
            'fields': ['priority', 'in_progress'],
        },
        {
            'fields': ['expire_time'],
        },
    ]

    use_dot_notation = True
    dot_notation_warning = True

class PriorityQueue(object):
    """A priority queue implementation using MongoDB

    :param connection: A connection to MongoDB.
    :param q_name: The name of the system wide queue to back this
    object with.
    """

    def __init__(self, connection, q_name):
        self.name = PRIORITY_QUEUE_PREFIX + q_name
        self.con = connection
        self.con.register([PriorityQueueDoc])

    @property
    def db(self):
        "FIXME: Should be passed in some way or other."
        return self.con.test

    @property
    def collection(self):
        return getattr(self.db, self.name)

    def push(self, priority, obj):
        pq = self.collection.PriorityQueueDoc()
        pq.priority = priority
        pq.task = obj
        pq.save()
        return pq._id

    def pop(self):
        try:
            rv = self.db.command(
                "findandmodify", self.name,
                query={
                    "in_progress": False,
                    "priority": {"$lte" : datetime.datetime.utcnow() }},
                sort={"priority" : 1},
                update={"$set": {"in_progress" : True}},
                )['value']
        except OperationFailure:
            return None

        # Set the expiration date before returning the value.
        expiration = self._calculate_expiration_time(rv['pending_life'])
        self.collection.update({'_id': self._parse_object_id(rv['_id'])},
                               {'$set': {'expire_time': expiration}})
        return rv

    def ack(self, id):
        "Drop a task with the given id."

        rv = self.collection.remove({'_id': self._parse_object_id(id), 'in_progress': True},
                                    safe=True)
        if rv['n'] is 0:
            raise KeyError

    def cancel(self, id):
        "Drop a task with the given id."

        rv = self.collection.remove({'_id': self._parse_object_id(id), 'in_progress': False},
                                    safe=True)
        if rv['n'] is 0:
            raise KeyError

    def _parse_object_id(self, id):
        "Return the object id or raise an error."
        try:
            return ObjectId(id)
        except pymongo.errors.InvalidId as e:
            raise errors.InvalidId(e)

    def _calculate_expiration_time(self, pending_life):
        return datetime.datetime.utcnow() + datetime.timedelta(seconds=pending_life)

def list_queues(db):
    """Return a list of the queues available on :param db:."""

    return [name[len(PRIORITY_QUEUE_PREFIX):] for name in db.collection_names()
            if name.startswith(PRIORITY_QUEUE_PREFIX)]
