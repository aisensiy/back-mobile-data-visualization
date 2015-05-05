#!/usr/bin/env python
# encoding: utf-8

from config import HOST, USER, PASSWD, DATABASE

dbconfig = {
    'host': HOST,
    'user': USER,
    'passwd': PASSWD,
    'db': DATABASE
}


def fetch_users(db, pagecnt=10000):
    offset = 0
    while True:
        users = db.fetchall("""
                            select uid from users where high > 4
                            limit %s offset %s""", (pagecnt, offset * pagecnt))
        if not users:
            break

        for uid in map(lambda x: x[0], users):
            yield uid

        offset += 1


def fetch_user_location_logs(uid, db):
    cols = ['start_time', 'location', 'day']
    prepare_sql = """select start_time, location, log_date
                        from location_logs_with_date
                        where uid = %s order by start_time"""
    data = db.fetchall(prepare_sql, (uid,))
    return [dict(zip(cols, row)) for row in data]
