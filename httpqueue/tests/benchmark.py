import argparse
import collections
import datetime
import httplib
import random
import threading

SERVER = 'localhost:8000'
Halt = False

# Totally not threadsafe...
counters = collections.defaultdict(int)

class ClientBase(threading.Thread):
    def __init__(self):
        self.conn = httplib.HTTPConnection(SERVER)
        self.counter = 0
        threading.Thread.__init__(self)

    def run(self):
        while not Halt:
            self.act()
            self.counter += 1

    def act(self):
        raise NotImplementedError

class Producer(ClientBase):
    def act(self):
        self.conn.request('POST', '/queue/foo/',
                          body='{"do":"something"}',
                          headers={'content-type': 'application/json',
                                   'x-httpqueue-priority': datetime.datetime.utcnow().isoformat(),
                                   })
        resp = self.conn.getresponse()
        if resp.status not in (200, 204):
            print "%s failed with %s" % (type(self).__name__, resp.status)
        else:
            counters['produced'] += 1

class Consumer(ClientBase):
    def act(self):
        self.conn.request('POP', '/queue/foo/')
        resp = self.conn.getresponse()
        id = resp.getheader('x-httpqueue-id')
        if resp.status == 200:
            counters['consumed'] += 1
        elif resp.status == 204:
            counters['no-content'] += 1
            return
        else:
            print "%s (POP) failed with %s" % (type(self).__name__, resp.status)
            return

        self.conn.request('ACK', '/queue/foo/id/%s' % id)
        resp = self.conn.getresponse()
        if resp.status in (200, 204):
            counters['acked'] += 1
        else:
            print "%s (ACK) failed with %s" % (type(self).__name__, resp.status)

parser = argparse.ArgumentParser(description='Run performance testing on httPQueue')
parser.add_argument('--producers', type=int, help='Number of producer threads to run', default=5)
parser.add_argument('--consumers', type=int, help='Number of consume/ack threads to run', default=10)
args = parser.parse_args()

start = datetime.datetime.now()
actors = set()

for i in range(args.producers):
    a = Producer()
    actors.add(a)
    a.start()

for i in range(args.consumers):
    a = Consumer()
    actors.add(a)
    a.start()

try:
    while True:
        pass
except KeyboardInterrupt:
    Halt = True

for a in actors:
    a.join()

period = datetime.datetime.now() - start

print '\nSpent %s; %s seconds' % (period, period.seconds)
for k, v in counters.items():
    print '%s = %s; %s per second' % (k, v, v/float(period.seconds))
