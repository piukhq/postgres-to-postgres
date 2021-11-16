# postgres-to-postgres

Copies a Postgres Database to another Postgres Server.

# Things I don't like about this project

## What on earth is `_convert_urls_to_dsns()`?

One use case of this tool is to move databases between "Azure Database for PostgreSQL Single Server" and "Azure Database for PostgreSQL Flexible Server". Flexible Server itself uses correctly formatted usernames, while Single Server does not. To have decent compatibility in both directions I decided to write this horrible hack.

Once we have stopped using "Azure Database for PostgreSQL Single Server" I'll remove this hack.

## Why no retry logic?

`pg_dump` and `pg_restore` can get quite shouty when going between different versions of Postgres, and rightly so. Unfortunately Microsoft does not offer PostgreSQL 13 everywhere yet, so I have shell_check disabled by default. Once Microsoft fully support PostgreSQL 13, or we drop use of Single Server internally, I'll add proper retries and error handling
