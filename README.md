# fscrawler

A fast, multi-threaded filesystem metadata crawler for Linux. Scans directory trees and writes file and directory metadata to PostgreSQL, a formatted table, or stdout.

## Features

- Work-stealing thread pool for parallel directory traversal
- Bulk ingestion via PostgreSQL binary COPY protocol
- Post-crawl foreign key enforcement and directory closure table for hierarchical queries
- Per-directory size statistics (direct and subtree bytes, file counts)

## Requirements

- Rust (edition 2024)
- PostgreSQL (for `--output postgres`)

## Build

```bash
cargo build --release
# binary at target/release/fscrawler
```

## Documentation

Generate and open the API docs locally:

```bash
cargo doc --open
```

## Database Setup

Before using `--output postgres` or any database flags, set up the roles, database, and schema permissions using the provided SQL scripts. Run them in order:

```bash
# as a PostgreSQL superuser
psql <superuser_url> -f sql/01_roles_database.sql

# as crawler_admin
psql <crawler_admin_url> -f sql/02_schema_permissions.sql
```

Then create the crawler tables:

```bash
fscrawler --create-tables --database-url <crawler_admin_url>
```

## Usage

```
fscrawler <ROOT> [OPTIONS]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--output <MODE>` | `stdout` | Output backend: `stdout`, `table`, or `postgres` |
| `--database-url <URL>` | — | PostgreSQL connection URL (required for `postgres` output and database flags) |
| `--workers <N>` | available CPUs | Number of worker threads |
| `--create-tables` | — | Create crawler tables and exit |
| `--clear` | — | Drop FK constraints, truncate all tables, re-initialise schema, and exit |

### Examples

Crawl a directory and print results to stdout:
```bash
fscrawler /data
```

Crawl and render a sorted table:
```bash
fscrawler /data --output table
```

Crawl into PostgreSQL:
```bash
fscrawler /data --output postgres --database-url "postgresql://crawler_admin:pass@localhost/crawler_db"
```

Clear all data and re-initialise the schema before a fresh crawl:
```bash
fscrawler --clear --database-url "postgresql://crawler_admin:pass@localhost/crawler_db"
fscrawler /data --output postgres --database-url "postgresql://crawler_admin:pass@localhost/crawler_db"
```

Use a fixed number of worker threads:
```bash
fscrawler /data --output postgres --workers 8 --database-url "postgresql://crawler_admin:pass@localhost/crawler_db"
```

## PostgreSQL Workflow

A full crawl run follows this sequence:

1. `--clear` — truncate existing data (if re-running)
2. Crawl — bulk COPY into `files` and `directories` (no FK constraints during ingestion)
3. Post-crawl — populate `users`, add FK constraints
4. Finish — build `directory_closure` and `directory_stats`

Steps 3 and 4 run automatically after a `--output postgres` crawl completes.

## Useful Queries

Cumulative size under a directory:
```sql
SELECT SUM(f.size_bytes)
FROM directory_closure dc
JOIN files f ON f.dir_id = dc.descendant_id
WHERE dc.ancestor_id = <dir_id>;
```

Per-user disk usage:
```sql
SELECT u.username, SUM(f.size_bytes) AS total_bytes
FROM files f
JOIN users u ON u.uid = f.owner_uid
GROUP BY u.username
ORDER BY total_bytes DESC;
```

Top directories by subtree size:
```sql
SELECT d.path, ds.subtree_bytes, ds.subtree_count
FROM directory_stats ds
JOIN directories d ON d.dir_id = ds.dir_id
ORDER BY ds.subtree_bytes DESC
LIMIT 20;
```

## Known Limitations

- **Re-run conflicts**: file and directory ID counters reset to 1 on each invocation. Always run `--clear` before re-crawling an existing database. A fix to seed the counters from the existing MAX is pending.
- Symlinks and special files are skipped.
- `add_usernames` (populate `users.username` from `/etc/passwd`) is implemented but not yet wired into the crawl pipeline.
