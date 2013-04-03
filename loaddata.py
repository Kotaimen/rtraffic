#!/usr/bin/env python
# -*- encoding: utf-8 -*-

'''
Insert a lot of dummy traffic data into a memcache cluster.

Created on Mar 29, 2013

@author: Kotaimen
'''

import eventlet
eventlet.monkey_patch(socket=True, select=True)
memcache = eventlet.import_patched('memcache')

import multiprocessing
import threading

import json
import math
import random
import time
import itertools
import datetime
import re

# Random words for payload, note: this comes from "import this"
WORDS = list(set(w.lower() for w in re.split(r'\W+', '''
The Zen of Python, by Tim Peters

Beautiful is better than ugly.
Explicit is better than implicit.
Simple is better than complex.
Complex is better than complicated.
Flat is better than nested.
Sparse is better than dense.
Readability counts.
Special cases aren't special enough to break the rules.
Although practicality beats purity.
Errors should never pass silently.
Unless explicitly silenced.
In the face of ambiguity, refuse the temptation to guess.
There should be one-- and preferably only one --obvious way to do it.
Although that way may not be obvious at first unless you're Dutch.
Now is better than never.
Although never is often better than *right* now.
If the implementation is hard to explain, it's a bad idea.
If the implementation is easy to explain, it may be a good idea.
Namespaces are one honking great idea -- let's do more of those!
''') if w))

VENDORS = ['TEST', 'PEST', 'PSET',
           'Bobo', 'Bob', 'Ray', 'Kotaimen', 'Tom', 'Harbinger',
           ]


MEMCACHE_SERVERS = ['127.0.0.1:11211', ]
# MEMCACHE_SERVERS = list('192.168.1.%d:11211' % i for i range(151, 158))
MEMCACHE_MAX_VALUE_LENGTH = 1024 * 1024 * 64
EVENTLET_POOL_SIZE = 1000
MULTISET_CHUNK_SIZE = 1000
MEMCACHE_ITEM_TIMEOUT = 3600
TOTAL_LINKS = 10000 * 100
WORKER_PROCESSES = 4
WORKER_CHUNK_SIZE = 10000 * 25
REPEAT_INSERT_COUNT = 10
REPEAT_INSERT_DELAY = 5 #60 * 1

def make_trafficinfo(start=1, end=10000, chunk=100):
    items = dict()
    now = time.time()
    for link_id in xrange(start, end):
        value = {
                'type': 'traffic-info',
                'link_id': link_id,
                'congestion': random.normalvariate(10, 5),
                'timestamp': now,
                'time': datetime.datetime.fromtimestamp(now).timetuple()[:6],
                'location': {
                            'type': 'Point',
                            'coordinates': [random.uniform(-180., 180.),
                                            random.uniform(-70., 85.)]
                            },
                'vendor': random.choice(VENDORS),
                'attributes': {
                                'payload': ' '.join(random.sample(WORDS, 15))
                              },
               }
        key = '%s-%s-%d' % (value['link_id'], value['vendor'], int(value['timestamp']))
        items[key] = json.dumps(value)
        if len(items) >= chunk:
            yield items
            items = dict()
            now = time.time()
    else:
        if items:
            yield items


def make_timeslot(items):
    for value in items.itervalues():
        now = json.loads(value)['time']
        key = 'idx-%s' % '-'.join(map(str,now))
        value = ','.join('%s' % k for k in items.iterkeys())
        return key, value


def memcache_writer(servers, ttl=MEMCACHE_ITEM_TIMEOUT):
    client = memcache.Client(servers,
        server_max_value_length=MEMCACHE_MAX_VALUE_LENGTH)
    def writer(items):
        client.set_multi(items, time=ttl)
        k, v = make_timeslot(items)
        client.add(k, '%s' % k, time=ttl) # dummy placeholder
        client.append(k, ',' + v, time=ttl)
    return writer


def loader(start, end):
    writer = memcache_writer(servers=MEMCACHE_SERVERS)
    pool = eventlet.greenpool.GreenPool(size=EVENTLET_POOL_SIZE)
    for items in make_trafficinfo(start, end, chunk=MULTISET_CHUNK_SIZE):
        pool.spawn_n(writer, items)
    pool.waitall()
    del pool

def loader2(args): # multiprocessing.Pool don't have starmap...
    print 'Loading %d~%d' % args
    loader(args[0], args[1])

def run(pool):
    total = TOTAL_LINKS
    chunk = WORKER_CHUNK_SIZE
    ranges = list((i, i + chunk) for i in range(0, total - 1, chunk))
    for i in range(0, REPEAT_INSERT_COUNT):
        print 'Take #%d' % (i + 1)
        for r in pool.imap_unordered(loader2, ranges):
            pass
        if REPEAT_INSERT_DELAY:
            print 'Sleeping...'
            time.sleep(REPEAT_INSERT_DELAY)


def main():
    pool = multiprocessing.Pool(processes=WORKER_PROCESSES)
    runner = threading.Thread(target=run, args=(pool,),)
    runner.daemon = True
    runner.start() # start in seperate thread so we can CTRL+C
    try:
        while True:
            runner.join(0.77)
            if not runner.isAlive():
                break
    except KeyboardInterrupt:
        print 'Aborting...'
        pool.terminate()
        pool.join()
    else:
        print 'Loaded.'
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()
