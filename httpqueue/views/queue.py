"""Queue view

This module is a Flask `Module` exposing the basic queue operations.
"""

import datetime

from flask import (Module, request, abort, json, jsonify,
                   make_response, current_app)

import httpqueue.model as model


PRIORITY_HEADER = 'X-httPQueue-Priority'
ID_HEADER = 'X-httPQueue-ID'

view = Module(__name__)

@view.route('/', methods=['GET'])
def list_queues():
    """Retrieve a JSON list of existing queues."""

    return json.dumps(model.queue.list_queues())

@view.route('/<q_name>/', methods=['POST'])
def push_item(q_name):
    """Take in a JSON document and add it to the queue.

    The request is also expected to have an X-httPQueue-Priority header.
    """

    try:
        priority = request.headers[PRIORITY_HEADER]
        priority = datetime.datetime.strptime(priority, '%Y-%m-%dT%H:%M:%S.%f')
    except KeyError:
        current_app.logger.error('No priority header.')
        abort(400)
    except ValueError as ex:
        current_app.logger.error(str(ex))
        abort(400)

    q = model.get_queue(q_name)
    try:
        task = json.loads(request.data)
    except ValueError:
        current_app.logger.error(
            'Failed to parse JSON from request body: %s' % request.data)
        abort(415)

    q.push(priority, task)
    return ''

@view.route('/<q_name>/', methods=['POP'])
def pop_item(q_name):
    """Remove and return the next item.

    The item is moved to the pending queue until it has been acked or
    its "pending lifetime" has been exceeded.
    """

    q = model.get_queue(q_name)
    q.restore_pending()
    item = q.pop()
    if item is None:
        response = make_response()
        response.status_code = 204
        return response

    response = make_response(jsonify(item['task']))
    response.headers[ID_HEADER] = item['_id']
    response.headers[PRIORITY_HEADER] = item['priority'].isoformat()
    return response

@view.route('/<q_name>/id/<id>', methods=['ACK'])
def ack_item(q_name, id):
    """Notify the service that a previously popped item is trash.
    """

    q = model.get_queue(q_name)

    try:
        q.ack(id)
    except KeyError:
        current_app.logger.error('Ack failed: no item with object id %s' % id)
        abort(404)
    except model.errors.InvalidId as ex:
        current_app.logger.error(str(ex))
        abort(404)
    return ''

@view.route('/<q_name>/id/<id>', methods=['CANCEL'])
def cancel_item(q_name, id):
    """Notify the service that a previously queued item should be dropped.

    If the document has already been picked up by a client a 404 is returned.
    """

    q = model.get_queue(q_name)

    try:
        q.cancel(id)
    except KeyError:
        current_app.logger.error('Ack failed: no item with object id %s' % id)
        abort(404)
    except model.errors.InvalidId as ex:
        current_app.logger.error(str(ex))
        abort(404)
    return ''
