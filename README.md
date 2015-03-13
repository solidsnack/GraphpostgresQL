# GraphpostgresQL -- a graph interface to relational data

GraphpostgresQL is inspired by Facebook's [`graphql`][graphql]. By using table
introspection, GraphpostgresQL is able to follow foreign keys and index into
complex datatypes like `json`, `jsonb` and `hstore`.

[graphql]: https://www.npmjs.com/package/graphql


# A Proof of Concept

GraphpostgresQL is alpha quality and has undergone neither extensive
optimization nor comprehensive testing. To use it for production workloads
would needlessly tempt fate.


# Install GraphpostgresQL

Using `psql`, load the `graphql` schema file:

```sql
\i graphql.sql
```

All definitions are created under the `graphql` schema. GraphpostgresQL
doesn't load any extensions or alter the `search_path`. If an older version of
GraphpostgresQL is loaded, the new installation will overwrite it.


# Using GraphpostgresQL

To generate a query, use `graphql.to_sql(text)`:

```sql
SELECT graphql.to_sql($$
  user("f3411edc-e1d0-452a-bc19-b42c0d5a0e36") {
    full_name,
    friendship
  }
$$);
```

Which should result in something like:

```sql
SELECT to_json("sub/2") AS "user"
  FROM "user",
       LATERAL (
         SELECT json_agg("user") AS friendship
           FROM "user"
           JOIN friendship ON (("user".id) = (friendship.second))
          WHERE (friendship.first)
              = ('f3411edc-e1d0-452a-bc19-b42c0d5a0e36'::uuid)
       ) AS "sub/1",
       LATERAL (
         SELECT "user".full_name, "sub/1".friendship
       ) AS "sub/2"
 WHERE (("user".id) = ('f3411edc-e1d0-452a-bc19-b42c0d5a0e36'::uuid))
```

To run a query, use `graphql.run(text)` instead of `graphql.to_sql(text)`.


# Removing GraphpostgresQL

It's easy to remove GraphpostgresQL:

```sql
DROP SCHEMA IF EXISTS graphql CASCADE;
```
