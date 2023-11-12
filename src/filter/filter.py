import os
import csv
import sys

import mariadb

from dotenv import load_dotenv

load_dotenv()
DB_HOST = os.environ['DB_HOST']
MARIADB_USER = os.environ['MARIADB_USER']
MARIADB_ROOT_PASSWORD = os.environ['MARIADB_ROOT_PASSWORD']
MARIADB_PORT = os.environ['MARIADB_PORT']
MARIADB_DATABASE = os.environ['MARIADB_DATABASE']

DATA_FILE_PATH = '/app/data/data.csv'


def main():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS people(name VARCHAR(256), age INT)')
    insert_csv_data(cur)

    conn.commit()
    cur.close()


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


def insert_csv_data(cur):
    with open(DATA_FILE_PATH) as f:
        csv_reader = csv.reader(f)
        next(csv_reader)
        for row in csv_reader:
            cur.execute('INSERT INTO people(name, age) VALUES (?, ?)', (row[0], int(row[1])))


if __name__ == '__main__':
    main()
