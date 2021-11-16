import logging
import re
import socket
import subprocess
from typing import Optional

import psycopg2
import redis
from pydantic import BaseSettings, PostgresDsn
from pythonjsonlogger import jsonlogger

logger = logging.getLogger()
logHandler = logging.StreamHandler()
logFmt = jsonlogger.JsonFormatter(timestamp=True)
logHandler.setFormatter(logFmt)
logger.addHandler(logHandler)


class Settings(BaseSettings):
    source_is_single_server: bool = False
    source_psql_connection_string: PostgresDsn
    destination_is_single_server: bool = False
    destination_psql_connection_string: PostgresDsn
    shell_check: bool = False
    leader_election_enabled: bool = False
    redis_url: Optional[str]


settings = Settings()


def _convert_urls_to_dsns() -> dict:
    data = {}

    single_server_regex = (
        r"[a-z]+\:\/\/(?P<user>[a-z]+\@[a-z.-]+):(?P<password>[A-z0-9]+)@(?P<host>[a-z.-]+)\/(?P<dbname>[a-z]+)"
    )
    flexible_server_regex = (
        r"[a-z]+\:\/\/(?P<user>[a-z]+):(?P<password>[A-z0-9]+)@(?P<host>[a-z.-]+)\/(?P<dbname>[a-z]+)"
    )

    if settings.source_is_single_server:
        source = re.search(single_server_regex, settings.source_psql_connection_string)
    else:
        source = re.search(flexible_server_regex, settings.source_psql_connection_string)

    if settings.destination_is_single_server:
        destination = re.search(single_server_regex, settings.destination_psql_connection_string)
    else:
        destination = re.search(flexible_server_regex, settings.destination_psql_connection_string)

    data["source"] = source.groupdict()
    data["source"]["dsn"] = (
        f"user={data['source']['user']} "
        f"password={data['source']['password']} "
        f"host={data['source']['host']} "
        f"dbname={data['source']['dbname']} "
        "sslmode=require"
    )
    data["destination"] = destination.groupdict()
    data["destination"]["dsn"] = (
        f"user={data['destination']['user']} "
        f"password={data['destination']['password']} "
        f"host={data['destination']['host']} "
        f"dbname={data['destination']['dbname']} "
        "sslmode=require"
    )
    return data


connection_strings = _convert_urls_to_dsns()
source_database = connection_strings["source"]["dbname"]
logging_extras = {"database": source_database}


def is_leader(dbname) -> bool:
    if settings.leader_election_enabled:
        r = redis.Redis.from_url(settings.redis_url)
        lock_key = f"postgres-to-postgres-{dbname}"
        hostname = socket.gethostname()
        is_leader = False

        with r.pipeline() as pipe:
            try:
                pipe.watch(lock_key)
                leader_host = pipe.get(lock_key)
                if leader_host in (hostname.encode(), None):
                    pipe.multi()
                    pipe.setex(lock_key, 10, hostname)
                    pipe.execute()
                    is_leader = True
            except redis.WatchError:
                pass
    else:
        is_leader = True
    return is_leader


def drop_create_database() -> None:
    conn = psycopg2.connect(connection_strings["destination"]["dsn"])
    conn.autocommit = True
    logging.warning(
        msg="Dropping and Recreating Database",
        extra=logging_extras,
    )
    with conn.cursor() as c:
        c.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{source_database}';")
        c.execute(f"DROP DATABASE IF EXISTS {source_database};")
        c.execute(f"CREATE DATABASE {source_database};")
    conn.close()


def sync_database() -> None:
    source_dsn = connection_strings["source"]["dsn"]
    destination_dsn = connection_strings["destination"]["dsn"]
    destination_database = destination_dsn.replace("dbname=postgres", f"dbname={source_database}")
    logging.warning(
        msg="Sync Start",
        extra=logging_extras,
    )
    subprocess.run(
        f"pg_dump --no-privileges --format=custom '{source_dsn}' | pg_restore --dbname='{destination_database}'",
        shell=True,
        check=settings.shell_check,
    )
    logging.warning(
        msg="Sync Complete",
        extra=logging_extras,
    )


if __name__ == "__main__":
    if is_leader(dbname=source_database):
        drop_create_database()
        sync_database()
    else:
        logging.warning(msg="Leader Election Failed, Skipping", extra={"database": source_database})
