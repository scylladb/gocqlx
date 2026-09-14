# Basic gocqlx Example

This example creates a keyspace and table, inserts a row with a table model, and reads it back with `SelectRelease`.

Run a local Scylla or Cassandra 4.0+ node. From the repository root, run:

```bash
cd examples/basic
go run . -hosts 127.0.0.1:9042
```

Use `-keyspace` to write to a different keyspace. Keyspace names may contain 1-48 ASCII letters, digits, or underscores.
