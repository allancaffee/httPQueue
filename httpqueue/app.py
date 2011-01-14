from flask import Flask, request, url_for, abort

import httpqueue.views.queue
import httpqueue.model

ENVVAR_NAME = 'RESQUE_SETTINGS'
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017

def make_app(config=None):
    app = Flask(__name__)
    if config:
        app.config.from_object(config)

    try:
        app.config.from_envvar(ENVVAR_NAME)
    except RuntimeError:
        print ENVVAR_NAME, 'not set. Using default configuration.'

    app.register_module(httpqueue.views.queue.view, url_prefix='/queue')
    httpqueue.views.queue.view.logger = app.logger
    httpqueue.model.init_model(app)
    return app

if __name__ == '__main__':
    make_app(__name__).run(debug=True)
