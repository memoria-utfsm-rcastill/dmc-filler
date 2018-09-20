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

        query = {'ts': date, 'dev': dev_id}

        hum = db.hum.find_one(query)
        prc = db.prc.find_one(query)
        tmp = db.tmp.find_one(query)
        wnd = db.wnd.find_one(query)

        doc = {'ts': date, 'dev': dev_id}
        if hum is None:
            doc['hum'] = prev_hum['val']
            doc['hum_ts'] = prev_hum['ts']
        else:
            doc['hum'] = hum['val']
            doc['hum_ts'] = date
            prev_hum = hum
        if prc is None:
            doc['prc'] = prev_prc['val']
            doc['prc_ts'] = prev_prc['ts']
        else:
            doc['prc'] = prc['val']
            doc['prc_ts'] = date
            prev_prc = prc
        if tmp is None:
            doc['tmp'] = prev_tmp['val']
            doc['tmp_ts'] = prev_tmp['ts']
        else:
            doc['tmp'] = tmp['val']
            doc['tmp_ts'] = date
            prev_tmp = tmp
        if wnd is None:
            doc['wnd'] = prev_wnd['val']
            doc['wnd_dir'] = prev_wnd.get('dir', 0)
            doc['wnd_ts'] = prev_wnd['ts']
        else:
            doc['wnd'] = wnd['val']
            doc['wnd_dir'] = wnd.get('dir', 0)
            doc['wnd_ts'] = date
            prev_wnd = wnd

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
