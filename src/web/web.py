import sys
import os
import mariadb

from dotenv import load_dotenv
from flask import Flask, jsonify

load_dotenv()
app = Flask(__name__)

DB_HOST = os.environ['DB_HOST']
WEB_HOST = os.environ['WEB_HOST']

MARIADB_PORT = os.environ['MARIADB_PORT']
WEB_PORT = os.environ['WEB_PORT']

MARIADB_USER = os.environ['MYSQL_USER']
MARIADB_ROOT_PASSWORD = os.environ['MYSQL_ROOT_PASSWORD']
MARIADB_DATABASE = os.environ['MYSQL_DATABASE']
PEOPLE_TABLE = os.environ['PEOPLE_TABLE']


@app.route('/',  methods=['GET'])
def index():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {PEOPLE_TABLE}')

    res = {PEOPLE_TABLE: [], 'status': 200}
    for name, age in cur:
        res[PEOPLE_TABLE].append((name, age))
    return jsonify(res)


@app.route('/health',  methods=['GET'])
def health():
    return jsonify({'status': 'OK'})


@app.errorhandler(404)
def not_found(_):
    return jsonify({'status': 404})


def get_connection():
    try:
        conn = mariadb.connect(
            host=DB_HOST,
            port=int(MARIADB_PORT),
            user=MARIADB_USER,
            password=MARIADB_ROOT_PASSWORD,
            database=MARIADB_DATABASE
        )
        return conn
    except mariadb.Error as e:
        print(f'Error connecting to MariaDB Platform: {e}')
        sys.exit(1)


if __name__ == '__main__':
    app.run(host=WEB_HOST, port=int(WEB_PORT), debug=True)
