#!/usr/bin/env python
# encoding: utf-8

from flask import Flask, jsonify, make_response
from json import dumps
from flask.ext.cors import CORS
import MySQLdb

app = Flask(__name__)
cors = CORS(app)

db = MySQLdb.connect('162.105.19.244', 'beijingmobile', 'KVision.im', 'mobile_data')


@app.route("/")
def hello():
    cursor = db.cursor()
    sql = 'select uid, day, hour, count from gprs_hour_counts limit 10'
    cursor.execute(sql)
    results = cursor.fetchall()
    return make_response(dumps(results))


if __name__ == "__main__":
    app.run(debug=True)
