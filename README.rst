httPQueue
======

httPQueue is a REST-based web service that manages JSON documents in
priority queues.  It stores the JSON itself in a Mongo database.  The
project itself is in a completely experimental state at the moment and
is not yet to ready be used.


Priorities
-------

The priority for an item is expected to be a date in ISO format and
should be specified in the ``X-httPQueue-Priority`` header.  No items
will be returned by the server until the specified date.

Acking Items
----------

When a message is queued it is given max pending lifetime.  If this
period of time is exceeded between when the item is popped without
anyone acking it the item is re-entered into priority queue.

Coming soon:

* This value can be set when the job is POSTed using the
  ``X-httPQueue-Pending-Life`` header (the value is specified in
  seconds).
