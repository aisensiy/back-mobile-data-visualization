#!/usr/bin/env python
# encoding: utf-8

from flask import Flask, make_response
from json import dumps
from flask.ext.cors import CORS
import MySQLdb
import MySQLdb.converters
from config import HOST, USER, PASSWD, DATABASE
from get_stop import get_stop, get_delta, get_stop_by_day, get_delta_by_day
from get_stop import date2str
from periodic_probability_matrix import generate_matrix
from get_most_proba_locations import get_most_proba_locations, pretty_print_most_proba_locations
from apriori import freq_seq_mining
from get_move import get_moves
from get_transient_entropy import transient_entropy, entropy
import pandas as pd
import datetime

app = Flask(__name__)
cors = CORS(app)

conv = MySQLdb.converters.conversions.copy()
# convert decimals to int
conv[246] = int
db = MySQLdb.connect(HOST, USER, PASSWD, DATABASE, conv=conv)

holidays = ['01', '07', '08', '14', '15', '21', '22', '28', '29']

@app.route("/usercount")
def usercount():
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """
    select count(1) from users where high = 5"""
    cursor.execute(prepare_sql)
    row = cursor.fetchone()
    return make_response(dumps(row[0]))


@app.route('/users/<uid>')
def user(uid):
    cols = ['uid', 'gender', 'age', 'brand_chn', 'call_fee', 'gprs_fee', 'dept_name']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """
    select uid, gender, age, brand_chn, call_fee, gprs_fee, dept_name
        from users where uid = %s"""
    cursor.execute(prepare_sql, (uid,))
    row = cursor.fetchone()
    result = dict(zip(cols, row))
    return make_response(dumps(result))


@app.route("/users/<int:offset>/<int:limit>")
def users(offset, limit):
    cols = ['uid', 'gender', 'age', 'brand_chn', 'call_fee', 'gprs_fee', 'dept_name']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """
    select uid, gender, age, brand_chn, call_fee, gprs_fee, dept_name
        from users where high = 5 limit %s offset %s"""
    cursor.execute(prepare_sql, (limit, offset))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/gprs_count_by_hour/<uid>")
def gprs_count_by_hour(uid):
    cols = ['uid', 'day', 'hour', 'count']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select uid, day, substring(minute, 3, 2) as hour, count(distinct minute) as count
                        from app_domain_logs where uid = %s group by day, hour"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/gprs_count_by_day/<uid>")
def gprs_count_by_day(uid):
    cols = ['uid', 'day', 'count']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select uid, day, sum(count) as count from gprs_hour_counts
                        where uid = %s group by day"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/location/daycount/<uid>")
def location_daycount_by_uid(uid):
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select distinct(log_date) as day
                        from location_logs_with_date
                        where uid = %s"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [row[0] for row in rows]
    return make_response(dumps(results))


@app.route("/location_by_uid/<uid>")
def location_by_uid(uid):
    cols = ['day', 'start_time', 'location']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select log_date as day, start_time, location
                        from location_logs_with_date
                        where uid = %s order by day, start_time"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(merge_locations(results)))


def _location_by_uid_stop(uid):
    cols = ['day', 'start_time', 'location']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select log_date as day, start_time, location
                        from location_logs_with_date
                        where uid = %s order by day, start_time"""
    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    locations = merge_locations(results)
    get_delta(locations)
    locations = get_stop(locations, 30)
    return locations


def _location_by_uid_stop_holiday(uid):
    locations = _location_by_uid_stop(uid)
    return [data for data in locations if  data['date'] in holidays]


def _location_by_uid_stop_workday(uid):
    locations = _location_by_uid_stop(uid)
    return [data for data in locations if  data['date'] not in holidays]


@app.route("/location_by_uid_stop/<uid>")
def location_by_uid_stop(uid):
    return make_response(dumps(_location_by_uid_stop(uid)))


@app.route("/raw_location_by_uid_day/<uid>/<day>")
def raw_location_by_uid_day(uid, day):
    day = '201312' + day
    cols = ['start_time', 'location']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select start_time, location
                        from location_logs_with_date
                        where uid = %s and log_date = %s order by start_time"""
    cursor.execute(prepare_sql, (uid, day))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/entropy_by_uid_day/<uid>/<day>")
def entropy_by_uid_day(uid, day):
    day = '201312' + day
    cols = ['start_time', 'location']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select start_time, location
                        from location_logs_with_date
                        where uid = %s and log_date = %s order by start_time"""
    cursor.execute(prepare_sql, (uid, day))
    rows = cursor.fetchall()
    results = merge_locations_by_date([dict(zip(cols, row)) for row in rows])
    get_delta_by_day(results)
    moves = get_moves(results)
    result = []
    for move in moves:
        for location in move:
            result.append({
                'entropy': transient_entropy(location, move),
                'time': location['start_time']
            })
    return make_response(dumps(result))


def get_speed_by_day(all_rows, day):
    timestamps = pd.date_range(start=day + '001500',
                               end=day + '235959',
                               freq='30Min')
    cols = ['start_time', 'location']
    speeds = []

    if len(all_rows) == 0:
        return speeds

    delta_t = 30
    for i in range(len(timestamps)):
        start_time = timestamps[i].to_datetime() - datetime.timedelta(minutes=delta_t / 2)
        end_time = timestamps[i].to_datetime() + datetime.timedelta(minutes=delta_t / 2)
        rows = filter(lambda x: date2str(start_time) <= x[0] <= date2str(end_time),
                      all_rows)
        if len(rows) == 0:
            speeds.append({
                'time': date2str(timestamps[i]),
                'speed': -1
            })
            continue
        rows = merge_locations_by_date([dict(zip(cols, row)) for row in rows])
        get_delta_by_day(rows)
        speed = entropy(rows, delta_t, [start_time, end_time])
        speeds.append({
            'time': date2str(timestamps[i]),
            'speed': speed
        })
    return speeds


@app.route("/speed_by_uid_day/<uid>/<day>")
def speed_by_uid_day(uid, day):
    day = '201312' + day
    speeds = []

    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select start_time, location
                        from location_logs_with_date
                        where uid = %s and log_date = %s order by start_time"""
    cursor.execute(prepare_sql, (uid, day))
    all_rows = cursor.fetchall()
    speeds = get_speed_by_day(all_rows, day)
    return make_response(dumps(speeds))


@app.route("/speed_by_uid/<uid>")
def speed_by_uid(uid):
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select start_time, location, log_date
                        from location_logs_with_date
                        where uid = %s order by start_time"""
    cursor.execute(prepare_sql, (uid,))
    all_rows = cursor.fetchall()
    result = []

    for day in range(1, 32):
        day = '201312%02d' % day
        rows_by_day = filter(lambda x: x[2] == day, all_rows)
        rows_by_day = map(lambda x: x[:2], rows_by_day)
        speeds = get_speed_by_day(rows_by_day, day)
        result.append(speeds)

    return make_response(dumps(result))


@app.route("/location_by_uid_day/<uid>/<day>")
def location_by_uid_day(uid, day):
    day = '201312' + day
    cols = ['start_time', 'location']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select start_time, location
                        from location_logs_with_date
                        where uid = %s and log_date = %s order by start_time"""
    cursor.execute(prepare_sql, (uid, day))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(merge_locations_by_date(results)))


@app.route("/location_by_uid_day_stop/<uid>/<day>")
def location_by_uid_day_stop(uid, day):
    day = '201312' + day
    cols = ['start_time', 'location']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select start_time, location
                        from location_logs_with_date
                        where uid = %s and log_date = %s order by start_time"""
    cursor.execute(prepare_sql, (uid, day))
    rows = cursor.fetchall()
    results = merge_locations_by_date([dict(zip(cols, row)) for row in rows])
    get_delta_by_day(results)
    results = get_stop_by_day(results)
    return make_response(dumps(results))


@app.route("/app_log_by_uid_day/<uid>/<day>")
def app_log_by_uid_day(uid, day):
    cols = ['minute', 'busi_name', 'app_name',
            'site_name', 'site_channel_name', 'domain', 'count']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select minute, busi_name, app_name, site_name, site_channel_name, domain, count
                            from app_domain_logs
                            where uid = %s and day = %s order by minute"""
    cursor.execute(prepare_sql, (uid, day))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/proba_matrix/<uid>")
def proba_matrix(uid):
    locations = _location_by_uid_stop(uid)
    return make_response(dumps(generate_matrix(locations)))


@app.route("/proba_matrix_holiday/<uid>")
def proba_matrix_holiday(uid):
    locations = _location_by_uid_stop_holiday(uid)
    return make_response(dumps(generate_matrix(locations)))


@app.route("/proba_matrix_workday/<uid>")
def proba_matrix_workday(uid):
    locations = _location_by_uid_stop_workday(uid)
    return make_response(dumps(generate_matrix(locations)))


@app.route("/most_proba_locations/<uid>")
def most_proba_locations(uid):
    locations = _location_by_uid_stop(uid)
    matrix = generate_matrix(locations)
    most_proba_locations = pretty_print_most_proba_locations(get_most_proba_locations(matrix))
    return make_response(dumps(most_proba_locations))

@app.route("/most_proba_locations_workday/<uid>")
def most_proba_locations_workday(uid):
    locations = _location_by_uid_stop_workday(uid)
    matrix = generate_matrix(locations)
    most_proba_locations = pretty_print_most_proba_locations(get_most_proba_locations(matrix))
    return make_response(dumps(most_proba_locations))


@app.route("/most_proba_locations_holiday/<uid>")
def most_proba_locations_holiday(uid):
    locations = _location_by_uid_stop_holiday(uid)
    matrix = generate_matrix(locations)
    most_proba_locations = pretty_print_most_proba_locations(get_most_proba_locations(matrix))
    return make_response(dumps(most_proba_locations))


def _stop_to_seq(locations):
    seqs = []
    for day in locations:
        seq = [stop['location'] for stop in day['locations']]
        if len(seq) > 0:
            seqs.append(seq)
    return seqs


@app.route("/freq_seq/<uid>")
def freq_seq(uid):
    import pprint
    locations = _location_by_uid_stop(uid)
    dataset = _stop_to_seq(locations)
    L, supportData = freq_seq_mining(dataset, 5)
    flattenL = []
    for ck in L:
        flattenL += ck
    flattenL = [seq for seq in flattenL if len(seq) > 1]
    return make_response(dumps(flattenL))


@app.route("/web_req_histgram/<uid>")
def web_req_histgram(uid):
    cols = ['hour', 'count']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select cast(substring(minute, 3, 2) as SIGNED) as hour, count(distinct minute) as count
                        from app_domain_logs where uid = %s group by hour"""

    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/site_count/<uid>")
def site_count(uid):
    cols = ['site_name', 'count']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select site_name, count(1) as count from app_domain_logs
                        where uid = %s group by site_name
                        order by count desc limit 10"""

    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/app_count/<uid>")
def app_count(uid):
    cols = ['app_name', 'count']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select app_name, count(1) as count from app_domain_logs
                        where uid = %s group by app_name
                        order by count desc limit 10"""

    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/call_histgram/<uid>")
def call_histgram(uid):
    cols = ['hour', 'count']
    db.ping(True)
    cursor = db.cursor()
    prepare_sql = """select hour(start_time) as hour, count(1) as count
                        from calls where uid = %s group by hour"""

    cursor.execute(prepare_sql, (uid,))
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(results))


@app.route("/semantic_data/<uid>")
def semantic_data(uid):
    locations = _location_by_uid_stop(uid)
    result = set()
    for day in locations:
        for item in day['locations']:
            result.add(item['location'])
    cols = ['location', 'station_desc', 'tags', 'addr', 'business']
    cursor = db.cursor()
    prepare_sql = """select location, station_desc, tags, addr, business from location_desc where location in (%s)""" % \
        ','.join(map(lambda x: "'" + x + "'", result))
    cursor.execute(prepare_sql)
    rows = cursor.fetchall()
    results = [dict(zip(cols, row)) for row in rows]
    return make_response(dumps(list(results)))



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
        'date': last_day[-2:],
        'locations': merge_locations_by_date(one_day_locations)
    })

    return group_by_day


if __name__ == "__main__":
    app.run(debug=True)
