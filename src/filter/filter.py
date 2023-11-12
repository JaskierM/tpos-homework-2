import os
import csv
import mariadb

from dotenv import load_dotenv

load_dotenv()

MARIADB_USER = os.environ['MARIADB_USER']
MARIADB_ROOT_PASSWORD = os.environ['MARIADB_ROOT_PASSWORD']
MARIADB_DATABASE = os.environ['MARIADB_DATABASE']


def main():
    cur = get_cursor()
    data = csv.reader('students.csv')
    cur.execute('CREATE TABLE IF NOT EXISTS people(name VARCHAR(256), age INT)')
    for row in data:
        cur.execute('INSERT INTO people(name, age) VALUES("%s", "%s")', row)

    print(cur.execute("SELECT * FROM people"))


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
    main()
