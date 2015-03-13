\include_relative graphql.sql
\include_relative fb/schema.sql
\include_relative fb/data.sql

\x auto

\C Consumers:
SELECT * FROM consumer;

\C Friendships:
SELECT friendship.*, l._||' -> '||r._ AS who FROM friendship,
         LATERAL (SELECT full_name FROM consumer WHERE first = id) AS l(_),
         LATERAL (SELECT full_name FROM consumer WHERE second = id) AS r(_);

DO $$
DECLARE
--graphql_q text = E'consumer("f3411edc-e1d0-452a-bc19-b42c0d5a0e36") {\n'
--                  '  full_name,\n'
--                  '  friendship\n'
--                  '}';
  graphql_q text = E'consumer("f3411edc-e1d0-452a-bc19-b42c0d5a0e36") {\n'
                    '  full_name,\n'
                    '  friendship { full_name }\n'
                    '}';
  sql_q text;
  result json;
  msg text;
BEGIN
  RAISE INFO E'GraphQL to parse:\n%\n', graphql_q;
  SELECT * INTO STRICT sql_q FROM graphql.to_sql(graphql_q);
  RAISE INFO E'SQL that will be run:\n%\n', sql_q;
  EXECUTE sql_q INTO STRICT result;
  RAISE INFO E'Result:\n%\n', result;
END
$$;

BEGIN;
SET LOCAL client_min_messages TO ERROR;
DROP SCHEMA graphql CASCADE;
DROP SCHEMA fb CASCADE;
END;
