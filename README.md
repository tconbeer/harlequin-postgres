# harlequin-postgres

This repo provides the Harlequin adapter for Postgres.

## Installation

`harlequin-postgres` depends on `harlequin`, so installing this package will also install Harlequin.

### Using pip

To install this adapter into an activated virtual environment:
```bash
pip install harlequin-postgres
```

### Using poetry

```bash
poetry add harlequin-postgres
```

### Using pipx

If you do not already have Harlequin installed:

```bash
pip install harlequin-postgres
```

If you would like to add the Postgres adapter to an existing Harlequin installation:

```bash
pipx inject harlequin harlequin-postgres
```

### As an Extra
Alternatively, you can install Harlequin with the `postgres` extra:

```bash
pip install harlequin[postgres]
```

```bash
poetry add harlequin[postgres]
```

```bash
pipx install harlequin[postgres]
```

## Usage and Configuration

You can open Harlequin with the Postgres adapter by selecting it with the `-a` option and passing a [Posgres DSN](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):

```bash
harlequin -a postgres "postgres://my-user:my-pass@localhost:5432/my-database"
```

You can also pass all or parts of the connection string as separate options. The following is equivalent to the above DSN:

```bash
harlequin -a postgres -h localhost -p 5432 -U my-user --password my-pass -d my-database
```

Many more options are available; to see the full list, run:

```bash
harlequin --help
```

For more information, see the [Harlequin Docs](https://harlequin.sh/docs/postgres/index).