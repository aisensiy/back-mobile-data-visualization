#!/usr/bin/env python
# encoding: utf-8

from flask import Flask, make_response
from json import dumps
from flask.ext.cors import CORS
import MySQLdb
from config import HOST, USER, PASSWD, DATABASE

app = Flask(__name__)
cors = CORS(app)

db = MySQLdb.connect(HOST, USER, PASSWD, DATABASE)


@app.route("/")
def hello():
    cursor = db.cursor()
    sql = 'select uid, day, hour, count from gprs_hour_counts limit 10'
    cursor.execute(sql)
    results = cursor.fetchall()
    return make_response(dumps(results))


if __name__ == "__main__":
    app.run(debug=True)
