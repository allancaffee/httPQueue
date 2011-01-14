from flask import Module, request, abort, url_for, json

import httpqueue.model as model

view = Module(__name__)

@view.route('/', methods=['GET'])
def list_queues():
    return json.dumps(model.queue.list_queues())

@view.route('/<q_name>/', methods=['POST'])
def push_item(q_name):
    try:
        priority = request.headers[PRIORITY_HEADER]
        priority = datetime.datetime.strptime(priority, '%Y-%m-%dT%H:%M:%S.%f')
    except KeyError:
        view.logger.error('No priority header.')
        abort(400)
    except ValueError as e:
        view.logger.error(str(e))
        abort(400)

    q = model.get_queue(q_name)
    try:
        task = json.loads(request.data)
    except ValueError:
        view.logger.error('Failed to parse JSON from request body: %s' % request.data)
        abort(415)

    q.push(priority, task)
    return ''

# FIXME: This really shouldn't be a GET request... Look for an
# alternative or make a new method POP.
@view.route('/<q_name>/', methods=['GET'])
def pop_item(q_name):
    """Remove and return the next item.

    The item is moved to the pending queue until it has been acked or
    its "pending lifetime" has been exceeded.
    """
    q = model.get_queue(q_name)
    # How do we return the Riak content to the user.
    model.pop_item()

@view.route('/<q_name>/', methods=['DELETE'])
def ack_item(q_name):
    """Notify the service that a previously popped item is trash.
    """
    q = model.get_queue(q_name)
    try:
        q.ack(request.headers['X-httPQueue-ID'])
    except KeyError:
        abort(404)
    return ''
