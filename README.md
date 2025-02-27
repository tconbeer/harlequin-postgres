# harlequin-postgres

This project provides the Harlequin adapter for Postgres. For more information, see [harlequin.sh](https://harlequin.sh/docs/postgres/index).


## Installation

You must install the `harlequin-postgres` package into the same environment as `harlequin`. The best and easiest way to do this is to use `uv` to install Harlequin with the `postgres` extra:

```bash
uv tool install 'harlequin[postgres]'
```

## Using Harlequin with Postgres

To connect to a Postgres database, run Harlequin with the `-a postgres` option and pass a [Posgres DSN](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) as an argument:

```bash
harlequin -a postgres "postgres://my-user:my-pass@localhost:5432/my-database"
```

## Connection Options

You can also pass all or parts of the connection string as separate options. The following is equivalent to the above DSN:

```bash
harlequin -a postgres -h localhost -p 5432 -U my-user --password my-pass -d my-database
```

The supported connection options are:

```
host
port
dbname
user
password
passfile
require_auth
channel_binding
connect_timeout
sslmode
sslcert
sslkey
```

For descriptions of each option, run:

```
harlequin --help
```

## Environment Variables

Harlequin's Postgres driver will load connection information from the standard `PG*` environment variables. Any options supplied at the command-line will override environment variables.


## Manual Transactions

To use Manual transaction mode, click on the label in the Run Query Bar to toggle the transaction mode from Auto to Manual.

## Further Documentation

For more information, see the [Harlequin Docs](https://harlequin.sh/docs/postgres/index).