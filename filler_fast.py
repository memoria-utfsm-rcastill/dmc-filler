import pymongo

from os import environ
from datetime import datetime, timedelta
from time import sleep
from sys import argv

ENV_DMC_MONGO = 'DMC_MONGO'


def usage():
    print('Usage: {prog} dev_id'.format(prog=argv[0]))
    exit(1)


def date_by_hour(*, from_dt, until_year):
    one_hour = timedelta(hours=1)
    current = from_dt
    while current.year <= until_year:
        yield current
        current += one_hour


def main():
    if len(argv) != 2 or not argv[1].isdecimal():
        usage()

    dev_id = int(argv[1])

    host = environ.get(ENV_DMC_MONGO, '192.168.8.210:27018')
    client = pymongo.MongoClient('mongodb://{ip}'.format(ip=host))
    db = client.dmc

    query = {'dev': dev_id, 'ts': {'$gte': datetime(
        2013, 1, 1, 0, 0, 0), '$lte': datetime(2017, 12, 31, 23, 59, 59)}}

    hums = iter(db.hum.find(query))
    prcs = iter(db.prc.find(query))
    tmps = iter(db.tmp.find(query))
    wnds = iter(db.wnd.find(query))

    hum = next(hums, None)
    prc = next(prcs, None)
    tmp = next(tmps, None)
    wnd = next(wnds, None)

    date_targets = list(date_by_hour(from_dt=datetime(
        2013, 1, 1, 0, 0, 0), until_year=2017))
    total = len(date_targets)

    prev_hum = {'ts': date_targets[0], 'val': 0, 'dev': dev_id}
    prev_prc = {'ts': date_targets[0], 'val': 0, 'dev': dev_id}
    prev_tmp = {'ts': date_targets[0], 'val': 0, 'dev': dev_id}
    prev_wnd = {'ts': date_targets[0], 'val': 0, 'dir': 0, 'dev': dev_id}

    docs = []

    for i, date in enumerate(date_targets):
        completed = (i/total)*100
        print('[%.2f%%] {ts}\r'.format(ts=date) % completed, end='')

        doc = {'ts': date, 'dev': dev_id}
        if hum is None or hum['ts'] > date:
            doc['hum'] = prev_hum['val']
            doc['hum_ts'] = prev_hum['ts']
        elif hum['ts'] == date:
            doc['hum'] = hum['val']
            doc['hum_ts'] = date
            prev_hum = hum
            hum = next(hums, None)
        else:
            raise AssertionError('hum ts < date')
        if prc is None or prc['ts'] > date:
            doc['prc'] = prev_prc['val']
            doc['prc_ts'] = prev_prc['ts']
        elif prc['ts'] == date:
            doc['prc'] = prc['val']
            doc['prc_ts'] = date
            prev_prc = prc
            prc = next(prcs, None)
        else:
            raise AssertionError('prc ts < date')
        if tmp is None or tmp['ts'] > date:
            doc['tmp'] = prev_tmp['val']
            doc['tmp_ts'] = prev_tmp['ts']
        elif tmp['ts'] == date:
            doc['tmp'] = tmp['val']
            doc['tmp_ts'] = date
            prev_tmp = tmp
            tmp = next(tmps, None)
        else:
            raise AssertionError('tmp ts < date')
        if wnd is None or wnd['ts'] > date:
            doc['wnd'] = prev_wnd['val']
            doc['wnd_dir'] = prev_wnd.get('dir', 0)
            doc['wnd_ts'] = prev_wnd['ts']
        elif wnd['ts'] == date:
            doc['wnd'] = wnd['val']
            doc['wnd_dir'] = wnd.get('dir', 0)
            doc['wnd_ts'] = date
            prev_wnd = wnd
            wnd = next(wnds, None)
        else:
            raise AssertionError('wnd ts < date')

        docs.append(doc)

    print()
    print('Inserting docs...', end=' ')
    db.data.insert_many(docs)
    print('Done')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print()
