# postgres-to-postgres

Copies a Postgres Database to another Postgres Server.

# Things I don't like about this project

## Database Connection Strings

Both `source_database_dsn` and `destination_database_dsn` require values where the database name is set to `{}` for example: `postgresql://username:password@someserver:5432/{}?sslmode=require`, we then use `source_database_name` and `destination_database_name` to set the actual database name. We do this to simplify the way we inject environment variables into a Kubernetes Cluster.

My intention is to come back to this and improve it, eventually.

## Why no retry logic?

`pg_dump` and `pg_restore` can get quite shouty when going between different versions of Postgres, and rightly so. Unfortunately Microsoft does not offer PostgreSQL 13 everywhere yet, so I have shell_check disabled by default. Once Microsoft fully support PostgreSQL 13, or we drop use of Single Server internally, I'll add proper retries and error handling
