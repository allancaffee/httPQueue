import unittest

import httpqueue.app


class TestingConfig(object):
    MONGODB_HOST = 'localhost'
    MONGODB_PORT = 27017

class BaseViewTest(unittest.TestCase):
    def setUp(self):
        self.app = httpqueue.app.make_app(TestingConfig)
        self.client = self.app.test_client()
