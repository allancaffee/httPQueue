import gevent.monkey
gevent.monkey.patch_all()

import collections
import datetime
import gevent
import httplib
import optparse
import random

SERVER = 'localhost:8000'
Halt = False

counters = collections.defaultdict(int)

class ClientBase(gevent.Greenlet):
    def __init__(self):
        gevent.Greenlet.__init__(self)
        self.conn = httplib.HTTPConnection(SERVER)

    def _run(self):
        while not Halt:
            self.act()
            gevent.sleep(0)

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
        resp.read() # Discard the body
        if resp.status not in (200, 204):
            print "%s failed with %s" % (type(self).__name__, resp.status)
        else:
            counters['produced'] += 1

class Consumer(ClientBase):
    def act(self):
        self.conn.request('POP', '/queue/foo/')
        resp = self.conn.getresponse()
        resp.read() # Discard the body
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
        resp.read() # Discard the body
        if resp.status in (200, 204):
            counters['acked'] += 1
        else:
            print "%s (ACK) failed with %s" % (type(self).__name__, resp.status)

parser = optparse.OptionParser(description='Run performance testing on httPQueue')
parser.add_option('--producers', '-p', type=int, help='Number of producer threads to run', default=5)
parser.add_option('--consumers', '-c', type=int, help='Number of consume/ack threads to run', default=10)
parser.add_option('--duration', '-d', type=int, default=None,
                  help='Number of seconds to run the test for (default is to run indefinitely')
options, args = parser.parse_args()

start = datetime.datetime.now()
actors = set()

for i in range(options.producers):
    a = Producer()
    actors.add(a)
    a.start()

for i in range(options.consumers):
    a = Consumer()
    actors.add(a)
    a.start()

try:
    if options.duration is None:
        gevent.joinall(actors)
    else:
        gevent.sleep(options.duration)
        Halt = True
except KeyboardInterrupt:
    Halt = True

gevent.joinall(actors)

period = datetime.datetime.now() - start

print '\nSpent %s; %s seconds' % (period, period.seconds)
print 'producers:', options.producers
print 'consumers:', options.consumers
for k, v in counters.items():
    print '%s = %s; %s per second' % (k, v, v/float(period.seconds))
