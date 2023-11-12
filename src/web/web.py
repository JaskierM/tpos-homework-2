import os
import mariadb

from dotenv import load_dotenv
from flask import Flask, jsonify

load_dotenv()
app = Flask(__name__)

MARIADB_USER = os.environ['MARIADB_USER']
MARIADB_ROOT_PASSWORD = os.environ['MARIADB_ROOT_PASSWORDD']
MARIADB_DATABASE = os.environ['MARIADB_DATABASE']


@app.route('/',  methods=['GET'])
def index():
    cur = get_cursor()
    cur.execute("SELECT * FROM people")
    return MARIADB_USER


@app.route('/health',  methods=['GET'])
def health():
    return jsonify({'status': 'OK'})


@app.errorhandler(404)
def not_found(_):
    return jsonify({'status': 404})


def get_cursor():
    try:
        conn = mariadb.connect(
            user=MARIADB_USER,
            password=MARIADB_ROOT_PASSWORD,
            host='localhost',
            port=3306,
            database=MARIADB_DATABASE
        )
        return conn.cursor()
    except mariadb.Error as e:
        print(f'Error connecting to MariaDB Platform: {e}')


if __name__ == '__main__':
    app.run(debug=True)
