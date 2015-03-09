#!/usr/bin/env python
# encoding: utf-8

from flask import Flask, make_response
from json import dumps
from flask.ext.cors import CORS
import MySQLdb
import MySQLdb.converters
from config import HOST, USER, PASSWD, DATABASE

app = Flask(__name__)
cors = CORS(app)

conv = MySQLdb.converters.conversions.copy()
# convert decimals to int
conv[246] = int
db = MySQLdb.connect(HOST, USER, PASSWD, DATABASE, conv=conv)


@app.route("/")
def hello():
    return "test"


@app.route("/gprs_count_by_hour/<uid>")
def gprs_count_by_hour(uid):
    cols = ['uid', 'day', 'hour', 'count']
    cursor = db.cursor()
    prepare_sql = """select uid, day, hour, count from gprs_hour_counts
                        where uid = %s"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/gprs_count_by_day/<uid>")
def gprs_count_by_day(uid):
    cols = ['uid', 'day', 'count']
    cursor = db.cursor()
    prepare_sql = """select uid, day, sum(count) as count from gprs_hour_counts
                        where uid = %s group by day"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/location_by_uid/<uid>")
def location_by_uid(uid):
    cols = ['day', 'start_time', 'location']
    cursor = db.cursor()
    prepare_sql = """select log_date as day, start_time, location
                        from location_logs_with_date
                        where uid = %s order by day, start_time"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(merge_locations(results)))


@app.route("/location_by_uid_day/<uid>/<day>")
def location_by_uid_day(uid, day):
    day = '201312' + day
    cols = ['start_time', 'location']
    cursor = db.cursor()
    prepare_sql = """select start_time, location
                        from location_logs_with_date
                        where uid = %s and log_date = %s order by start_time"""
    cursor.execute(prepare_sql, (uid, day))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(merge_locations_by_date(results)))


@app.route("/app_log_by_uid_day/<uid>/<day>")
def app_log_by_uid_day(uid, day):
    cols = ['minute', 'busi_name', 'app_name',
            'site_name', 'site_channel_name', 'domain', 'count']
    cursor = db.cursor()
    prepare_sql = """select minute, busi_name, app_name, site_name, site_channel_name, domain, count
                            from app_domain_logs
                            where uid = %s and day = %s order by minute"""
    cursor.execute(prepare_sql, (uid, day))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


def merge_locations_by_date(logs):
    result = []
    location = None
    start_time = None
    last_start_time = None
    for log in logs:
        cur_start_time = log['start_time']
        cur_location = log['location']
        if location is None:
            location = cur_location
            start_time = cur_start_time
        elif location != cur_location:
            result.append({
                'start_time': start_time,
                'end_time': last_start_time,
                'location': location
            })
            location = cur_location
            start_time = cur_start_time
        last_start_time = cur_start_time

    result.append({
        'start_time': start_time,
        'end_time': last_start_time,
        'location': location
    })
    return result


def merge_locations(logs):
    group_by_day = []
    last_day = None
    one_day_locations = []
    for log in logs:
        cur_day = log['day']
        if last_day is not None and last_day != cur_day:
            group_by_day.append({
                'date': last_day[-2:],
                'locations': merge_locations_by_date(one_day_locations)
            })
            one_day_locations = []
        last_day = cur_day
        one_day_locations.append(log)

    group_by_day.append({
        'date': last_day,
        'locations': merge_locations_by_date(one_day_locations)
    })

    return group_by_day



if __name__ == "__main__":
    app.run(debug=True)
