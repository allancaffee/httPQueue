import datetime

from flask import Module, request, abort, url_for, json, jsonify, make_response

import httpqueue.model as model


PRIORITY_HEADER = 'X-httPQueue-Priority'
ID_HEADER = 'X-httPQueue-ID'

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
    item = q.pop()
    if item is None:
        response = make_response()
        response.status_code = 204
        return response

    response = make_response(jsonify(item['task']))
    response.headers[ID_HEADER] = item['_id']
    response.headers[PRIORITY_HEADER] = item['priority'].isoformat()
    return response

@view.route('/<q_name>/', methods=['DELETE'])
def ack_item(q_name):
    """Notify the service that a previously popped item is trash.
    """
    q = model.get_queue(q_name)
    print request.headers
    if ID_HEADER not in request.headers:
        view.logger.error('Ack failed: %s header not present' % ID_HEADER)
        abort(400)
    id = request.headers[ID_HEADER]

    try:
        q.ack(id)
    except KeyError:
        view.logger.error('Ack failed: no item with object id %s' % id)
        abort(404)
    except model.errors.InvalidId as e:
        view.logger.error(str(e))
        abort(404)
    return ''
