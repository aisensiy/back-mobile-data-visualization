#!/usr/bin/env python
# encoding: utf-8

from config import HOST, USER, PASSWD, DATABASE
from db import MySQL as DB
from app import get_speed_by_day
import sys


dbconfig = {
    'host': HOST,
    'user': USER,
    'passwd': PASSWD,
    'db': DATABASE
}


def fetch_user_logs(uid, db):
    prepare_sql = """select start_time, location, log_date
                        from location_logs_with_date
                        where uid = %s order by start_time"""
    return db.fetchall(prepare_sql, (uid,))


def run():
    output = open(sys.argv[1], 'w')
    db = DB(dbconfig)
    pagecnt = 10000
    offset = 0
    while True:
        users = db.fetchall("""
                            select uid from users where high > 4
                            limit %s offset %s""", (pagecnt, offset * pagecnt))
        if not users:
            break

        for uid in map(lambda x: x[0], users):
            data = fetch_user_logs(uid, db)
            for day in range(1, 32):
                day = '201312%02d' % day
                rows_by_day = filter(lambda x: x[2] == day, data)
                rows_by_day = map(lambda x: x[:2], rows_by_day)
                speeds = map(lambda x: x['speed'],
                             get_speed_by_day(rows_by_day, day))
                output.write(','.join(map(str, [uid, day] + speeds)) + '\n')
        offset += 1
    output.close()

if __name__ == '__main__':
    run()
