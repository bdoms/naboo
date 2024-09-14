# Naboo

A light-weight, asynchronous ORM-like wrapper around `asyncpg` targeting Python 3.12+.

All records are returned as dictionaries, because you're just going to encode to JSON anyway.


## Testing

Get into postgres:

```bash
sudo -u postgres psql
```

Then setup the test database and permissions (or modify the environment variables in `pytest.ini` instead):

```sql
CREATE DATABASE naboo_test;
CREATE USER naboo_test_user WITH PASSWORD 'naboo_test_password';
GRANT ALL PRIVILEGES ON DATABASE naboo_test TO naboo_test_user;
```

Then to run tests:

```bash
pytest
```
