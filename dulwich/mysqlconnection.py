import os
import urlparse
import mysql.connector


def _parse(url):
    """Parses a database URL."""
    url = urlparse.urlparse(url)
    # Remove query strings.
    path = url.path[1:]
    path = path.split('?', 2)[0]
    config = {
        'host': url.hostname or '',
        'port': url.port or 3306,
        'database': path or '',
        'user': url.username or '',
        'password': url.password or '',
    }
    return config


DB_CONFIG = 'DB_URL' in os.environ and _parse(os.environ['DB_URL']) or {}


def set_db_url(url):
    DB_CONFIG = _parse(url)


connection_pool = None

POOL_NAME = "PORTIA"

POOL_SIZE = 8

USE_PREPARED_STATEMENTS = False


def get_connection():
    global connection_pool
    if not connection_pool:
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
			pool_name=POOL_NAME,
			pool_size = POOL_SIZE,
			**DB_CONFIG)
    connection = connection_pool.get_connection()
    cursor = connection.cursor()
    cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;')
    cursor.close()
    return connection


def dbcursor(func):
    '''A decorator that fully manages the db connection and cursor.

    It gets a connection from the pool and instantiates a cursor that is
    passed to the decorated function. If an exception is thrown, it rollbacks
    the db changes. In other case, it closes the cursor, commits any pending
    changes and returns the connection to the pool.
    '''

    def wrapper(*args, **kwargs):
        connection = get_connection()
        cursor = connection.cursor(prepared=USE_PREPARED_STATEMENTS)
        kwargs['cursor'] = cursor
        try:
            retval = func(*args, **kwargs)
        except:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.commit()
            connection.close()
        return retval

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper
