import os
import csv
import sys

import mariadb

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ['DB_HOST']
MARIADB_PORT = os.environ['MARIADB_PORT']
MARIADB_USER = os.environ['MARIADB_USER']
MARIADB_ROOT_PASSWORD = os.environ['MARIADB_ROOT_PASSWORD']
MARIADB_DATABASE = os.environ['MARIADB_DATABASE']
PEOPLE_TABLE = os.environ['PEOPLE_TABLE']

DATA_FILE_PATH = '/app/data/data.csv'


def main():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f'CREATE TABLE IF NOT EXISTS {PEOPLE_TABLE}(name VARCHAR(256), age INT)')
    insert_csv_data(cur)
    check_insert(cur)

    conn.commit()
    cur.close()


def get_connection():
    try:
        conn = mariadb.connect(
            host=DB_HOST,
            port=int(MARIADB_PORT),
            user=MARIADB_USER,
            password=MARIADB_ROOT_PASSWORD,
            database=MARIADB_DATABASE,
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
            cur.execute(f'INSERT INTO {PEOPLE_TABLE}(name, age) VALUES (?, ?)', (row[0], int(row[1])))


def check_insert(cur):
    cur.execute(f'SELECT * FROM {PEOPLE_TABLE}')

    print('Table content after the fill script:')
    for name, age in cur:
        print(f'Name: {name}, Age: {age}')


if __name__ == '__main__':
    main()
