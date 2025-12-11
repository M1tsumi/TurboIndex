# TurboIndex

TurboIndex is a modern MySQL query optimization tool. It profiles queries, suggests safer rewrites, and recommends indexes for legacy and modern MySQL / MariaDB deployments.

## Status

Early prototype / MVP. Interfaces and output formats may change.

## Requirements

- Python 3.9+
- MySQL / MariaDB instance you can connect to

## Installation (editable dev install)

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
```

## CLI Usage (MVP)

Profile a query:

```bash
turboindex profile \
  --host localhost --port 3306 \
  --user root --password secret --database mydb \
  --query "SELECT * FROM users WHERE id = 42"
```

Rewrite a query (safe mode):

```bash
turboindex rewrite \
  --mode safe \
  --query "SELECT * FROM users WHERE status = 'a' OR status = 'b'"
```

Recommend indexes for a query:

```bash
turboindex recommend-indexes \
  --host localhost --port 3306 \
  --user root --password secret --database mydb \
  --query "SELECT * FROM orders WHERE customer_id = 123"
```

See `turboindex --help` for full options.

## Configuration

TurboIndex can read defaults from a `turboindex.toml` file in the current
directory and from environment variables. Command-line flags always win
over config/env.

### `turboindex.toml` example

Create a file named `turboindex.toml` next to where you run the CLI:

```toml
[mysql]
host = "localhost"
port = 3306
user = "root"
password = "secret"
database = "mydb"
version = "mysql_5.7"
```

### Environment variables

You can override or provide settings via env vars:

- `TURBOINDEX_HOST`
- `TURBOINDEX_PORT`
- `TURBOINDEX_USER`
- `TURBOINDEX_PASSWORD`
- `TURBOINDEX_DATABASE`
- `TURBOINDEX_MYSQL_VERSION`

### Using the CLI with config

With `turboindex.toml` (or env vars) in place, you can omit DSN flags:

```bash
turboindex profile --query "SELECT * FROM users WHERE id = 42"

turboindex recommend-indexes --query "SELECT * FROM orders WHERE customer_id = 123"
```
