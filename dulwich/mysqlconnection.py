import os
import urlparse

from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector.errors import DatabaseError, PoolError


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
    global DB_CONFIG
    DB_CONFIG = _parse(url)


connection_pool = None
POOL_NAME = "PORTIA"
POOL_SIZE = 8
USE_PREPARED_STATEMENTS = False


def get_connection():
    global connection_pool
    if not connection_pool:
        connection_pool = MySQLConnectionPool(
            pool_name=POOL_NAME,
            pool_size=POOL_SIZE,
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
            try:
                connection.close()
            except PoolError:
                # Connections were replenished so this one can be discarded
                pass
        return retval

    wrapper.__name__, wrapper.__doc__ = func.__name__, func.__doc__
    return wrapper


def replenishing_cursor(func):
    '''When no connections are available refill the queue to handle connections
    that may have been destroyed accidentally'''
    def wrapper(*args, **kwargs):
        try:
            return dbcursor(func)(*args, **kwargs)
        except PoolError:
            for i in range(POOL_SIZE):
                if connection_pool._cnx_queue.full():
                    break
                connection_pool.add_connection()
            return dbcursor(func)(*args, **kwargs)

    wrapper.__name__, wrapper.__doc__ = func.__name__, func.__doc__
    return wrapper


def retry_operation(retries=3, catches=(DatabaseError,)):
    '''
    :param retries: Number of times to attempt the operation
    :param catches: Which exceptions to catch and trigger a retry
    '''

    def wrapper(func):

        def wrapped(*args, **kwargs):
            err = None
            for _ in range(retries):
                try:
                    return func(*args, **kwargs)
                except catches as e:
                    err = e
            raise err
        wrapped.__name__, wrapped.__doc__ = func.__name__, func.__doc__
        return wrapped

    return wrapper
