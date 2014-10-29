from contextlib import closing
import logging

import psycopg2

logging.disable(logging.CRITICAL)


def setup():
    # Connect to the default DB so we can create the test
    pg_conn = psycopg2.connect(database='postgres', host='127.0.0.1')
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with closing(pg_conn):
        with closing(pg_conn.cursor()) as cursor:
            cursor.execute(
                'DROP DATABASE IF EXISTS {}'.format('test_hermes'))
            cursor.execute('CREATE DATABASE {}'.format('test_hermes'))


def teardown():
    pg_conn = psycopg2.connect(database='postgres', host='127.0.0.1')
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with closing(pg_conn):
        with closing(pg_conn.cursor()) as cursor:
            cursor.execute(
                'DROP DATABASE IF EXISTS {}'.format('test_hermes'))
