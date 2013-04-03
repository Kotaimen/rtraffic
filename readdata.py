#!/usr/bin/env python
# -*- encoding: utf-8 -*-

'''
Insert a lot of calculated traffic data from couchbase cluster

Created on Apr 3, 2013

@author: Kotaimen
'''

import gevent.monkey
gevent.monkey.patch_all()
import gevent.pool
import itertools
import couchbase
import datetime
import time

def main():
    connection = couchbase.client.Couchbase('192.168.1.154:8091', 'default', '');
    bucket = connection['default']

    now = int(time.time())
    start_time = now - 30 * 60

    def read_traffic(key):
        for row in bucket.view('_design/ray/_view/live_congestion',
            limit=100,
            stale='ok',
            startkey=[key, start_time],
            endkey=[key, now + 1],
            ):
            if row is None or 'value' not in row:
                return 'now found'
            value = row['value']
            return 'key=%d, record_count=%d, average_congestion=%.2f, age=%.2fs' % \
                (key, value['count'], value['congestion'],
                      now - value['age'])

    pool = gevent.pool.Pool(size=200)
    for result in pool.imap_unordered(read_traffic,
                                      itertools.cycle(xrange(0, 10000 * 50, 100))):

        print result
    pool.join()

if __name__ == '__main__':
    main()
