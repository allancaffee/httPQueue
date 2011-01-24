"""Mongo backed queue model"""

import datetime

from mongokit import Document, ObjectId, OperationFailure
import pymongo.errors

import httpqueue.model.errors

# The prefix added to collections that belong to priority queues.
PRIORITY_QUEUE_PREFIX = 'pq_'

class PriorityQueueDoc(Document):
    """Document representing a task and its associated metadata."""

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
        return pq['_id']

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
        """Drop a task with the given id that has already been picked up."""

        rv = self.collection.remove({
            '_id': self._parse_object_id(id), 'in_progress': True}, safe=True)
        if rv['n'] is 0:
            raise KeyError

    def cancel(self, id):
        """Drop a task with the given id given that it has not yet been popped.
        """

        rv = self.collection.remove({
            '_id': self._parse_object_id(id), 'in_progress': False}, safe=True)
        if rv['n'] is 0:
            raise KeyError

    def restore_pending(self):
        """Restore all pending tasks whose pending-life has expired.

        They will be returned to the state they were in prior to being
        POPed.  This should be run as frequently as is feasable so
        that jobs can be picked back up by a worker quickly.
        """

        self.collection.update(
            {'in_progress': True,
             'expire_time': {'$lte': datetime.datetime.utcnow()}},
            {'$unset': {'expire_time': 1},
             '$set': {'in_progress': False}})

    def _parse_object_id(self, id):
        """Return the object id or raise an error."""
        try:
            return ObjectId(id)
        except pymongo.errors.InvalidId as ex:
            raise httpqueue.model.errors.InvalidId(ex)

    def _calculate_expiration_time(self, pending_life):
        """Calculate the expiration date based on the pending life."""
        return datetime.datetime.utcnow() + \
               datetime.timedelta(seconds=pending_life)

def list_queues(db):
    """Return a list of the queues available on :param db:."""

    return [name[len(PRIORITY_QUEUE_PREFIX):] for name in db.collection_names()
            if name.startswith(PRIORITY_QUEUE_PREFIX)]
