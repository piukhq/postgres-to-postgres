import logging
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
    source_database_dsn: PostgresDsn
    source_database_name: str
    destination_database_dsn: PostgresDsn
    destination_database_name: str
    shell_check: bool = False
    leader_election_enabled: bool = False
    redis_url: Optional[str]
    extra_dump_args: str = ""
    extra_restore_args: str = ""


settings = Settings()
source_dsn = settings.source_database_dsn.format(settings.source_database_name)
destination_dsn = settings.destination_database_dsn.format(settings.destination_database_name)
logging_extras = {"database": settings.source_database_name}


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
    conn = psycopg2.connect(settings.destination_database_dsn.format("postgres"))
    conn.autocommit = True
    logging.warning(
        msg="Dropping and Recreating Database",
        extra=logging_extras,
    )
    with conn.cursor() as c:
        c.execute(
            f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity WHERE datname = '{settings.destination_database_name}';
            """
        )
        c.execute(f"DROP DATABASE IF EXISTS {settings.destination_database_name};")
        c.execute(f"CREATE DATABASE {settings.destination_database_name};")
    conn.close()


def sync_database() -> None:
    pg_dump_command = f"pg_dump {settings.extra_dump_args} --format=custom '{source_dsn}'"
    pg_restore_command = f"pg_restore {settings.extra_restore_args} --no-owner --dbname='{destination_dsn}'"
    command = f"{pg_dump_command} | {pg_restore_command}"
    logging.warning(
        msg="Sync Start",
        extra=logging_extras,
    )
    subprocess.run(
        command,
        shell=True,
        check=settings.shell_check,
    )
    logging.warning(
        msg="Sync Complete",
        extra=logging_extras,
    )


if __name__ == "__main__":
    if is_leader(dbname=settings.source_database_name):
        drop_create_database()
        sync_database()
    else:
        logging.warning(msg="Leader Election Failed, Skipping", extra=logging_extras)
