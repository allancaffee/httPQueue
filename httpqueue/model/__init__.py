import mongokit

import queue

connection = None

def init_model(app):
    global connection
    connection = mongokit.Connection(app.config['MONGODB_HOST'],
                                     app.config['MONGODB_PORT'])

def get_queue(q_name):
    return queue.PriorityQueue(connection, q_name)
