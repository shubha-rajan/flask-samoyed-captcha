"""Miscellaneous utility functions for starter project.
"""
import platform
from typing import Any, Dict

import sqlalchemy # type: ignore

from config import CSQL_CONNECTION, DB_USER, DB_PWD, DB_NAME


def cloudsql_postgres(
    *,
    instance: str=CSQL_CONNECTION,
    username: str=DB_USER,
    password: str=DB_PWD,
    database: str=DB_NAME,
    driver: str = "postgres+pg8000",
    pool_size: int = 5,
    max_overflow: int = 2,
    pool_timeout: int = 30,
    pool_recycle: int = 1800,
) -> Any:
    """Creates a SQLAlchemy connection for a Cloud SQL Postgres instance.

    Args:
        instance: Cloud SQL instance name (project:region:instance)
        username: database user to connect as
        password: password for username
        database: name of the database within the Cloud SQL instance
        driver: driver name
        poolsize: maximum number of permanent connections
        max_overflow: number of connections to temporarily exceed pool_size
                      if no connections available
        pool_timeout: maximum # seconds to wait for a new connection
        pool_recycle: number of seconds until a connection will be recycled

    Returns:
        A SQLAlchemy connection instance created with create_engine.
        We assume that if this code is running on Windows (for local dev/test)
        then we're connecting to Cloud SQL via the proxy, so need to use
        localhost instead of a Unix socket for the connection.

    Note that default settings from config.py are used, so for most cases the
    caller doesn't need to explicitly specify any settings.
    """

    if platform.system() == "Windows":
        connection_string = f"{driver}://postgres:{password}@127.0.0.1:5432/{database}"
    else:
        # If not Windows, we assume a Linux-compatible OS.
        unix_socket: Dict[str, str] = {
            "unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(instance)
        }
        connection_string = sqlalchemy.engine.url.URL(
            drivername=driver,
            username=username,
            password=password,
            database=database,
            query=unix_socket,
        )

    return sqlalchemy.create_engine(
        connection_string,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
    )
