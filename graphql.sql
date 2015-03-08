BEGIN;

DROP SCHEMA IF EXISTS graphql CASCADE;
CREATE SCHEMA graphql;

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //

# Syntax

The form of a GraphQL query is either a simple selector or a selector with
nested selectors or GraphQL queries.

    <graphQL> = <selector>
              | <selector> { <graphQL>* }

Selectors are of three kinds:

* "table selectors" that specify a datatype or collection and an ID. For
  example: `user('606fa027-a577-4018-952e-3c8469372829')`. More formally, the
  syntax of a table selector is:

      <selector/collection> = <collection-name> '(' <id> ')'

  Maybe someday, we'll extend the `<id>` portion to encompass predicates, to
  allow for queries like:

      user(created <= '2011-10-01')

* "column selectors" that specify a field name. Like: `full_name`.

* A "curious blend" of field and table selectors that perform JOINs, in a
  (hopefully) intuitive way. For example:

      user('606fa027-a577-4018-952e-3c8469372829') {
        friendship { // Uses the friendship table to find users
          id,
          full_name
        }
        post { // Uses the post table to find posts
          title
        }
      }

# Semantics

At the root of the query, there must be a table selector. Sub-queries are
taken relative to the super query.

Queries over collections (tables) are implicitly array-valued.

Nested selection is allowed for columns of JSON type, HStore type and of row
type.


// * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

SET LOCAL search_path TO graphql; -- Definitions will be created in this schema
--- However, we must still qualify references between functions -- as when
--- to_sql calls parse_many -- because the search_path will be different when
--- the code is run by the application/user.

CREATE FUNCTION to_sql(expr text)
RETURNS TABLE (query text) AS $$
BEGIN
  RETURN QUERY SELECT graphql.to_sql(selector, predicate, body)
                 FROM graphql.parse_many(expr);
END
$$ LANGUAGE plpgsql STABLE STRICT;

CREATE FUNCTION to_sql(selector text, predicate text, body text)
RETURNS text AS $$
DECLARE
  q text;
  tab regclass = selector::regclass;       -- Without a parent, we need a table
  cols text[];
  col text;
  sub record;
  pk text = NULL;
  fk record;
  subselects text[];
BEGIN
  q := 'FROM ' || tab;                              -- Regclass is auto-escaped
  body := substr(body, 2, length(body)-2);
  IF predicate IS NOT NULL THEN
    SELECT array_to_string(array_agg(format('%I', col)), ', ')
      FROM unnest(graphql.pk(tab)) INTO pk;
    SELECT array_to_string(array_agg(format('%I', col)), ', ')
      FROM graphql.pk(tab) INTO pk;
    q := q || E'\n WHERE (' || pk || ') = (' || predicate || ')';
    --- Compound primary keys are okay, since we naively trust the input...
  END IF;
  FOR sub IN SELECT * FROM graphql.parse_many(body) LOOP
    IF sub.predicate IS NOT NULL THEN
      RAISE EXCEPTION 'Unhandled nested selector %(%)',
                      sub.selector, sub.predicate;
    END IF;
    SELECT col FROM cols(tab) WHERE col = sub.selector INTO col;
    CASE
    WHEN FOUND AND sub.body IS NULL THEN           -- A simple column reference
      SELECT * FROM graphql.fk(tab)
       WHERE cols[1] = col AND cardinality(cols) = 1
        INTO fk; -- TODO: If there's more than one, emit a clear message.
      IF FOUND THEN
        subselects := subselects
                   || format(E'SELECT to_json(%1$I) AS %4$I FROM %1$I\n'
                              ' WHERE %1$I.%2$I = %3$I.%4$I',
                             fk.other, fk.refs[1], tab, col);
        cols := cols || format('%I.%I', 'sub/'||cardinality(subselects), col);
      ELSE
        cols := cols || format('%I', col);
      END IF;
    WHEN FOUND AND sub.body IS NOT NULL THEN             -- Index into a column
      --- TODO: Handle nested lookup into JSON, HStore, RECORD
      --- TODO: If col REFERENCES something, push lookup down to it
    WHEN NOT FOUND THEN             -- It might be a reference to another table
      SELECT fk.*
        FROM graphql.fk(sub.selector),
             LATERAL (SELECT num FROM graphql.cols(tab)
                       WHERE col = fk.cols[1]) AS _
       WHERE cardinality(fk.cols) = 1 AND fk.tab = to_sql.tab
       ORDER BY _.num LIMIT 1 INTO fk;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'Not able to construct a JOIN for missing column: %',
                        sub.selector;
      END IF;
      --- If:
      --- * Thare are two and only two foreign keys for the other table, and
      --- * All the columns of the table participate in one or the other
      ---   foreign key, then
      --- * We can treat the table as a JOIN table and follow the keys.
      --- Otherwise:
      --- * We use the existence of the foreign key to look up the record in
      ---   the table that JOINs with us.
      --- Whenever we are looking at a table that REFERENCES us, we assume it
      --- is a many-to-one relationship; and expect to return an array-valued
      --- result.
      IF FALSE THEN
        --- Recursion happens in here
      ELSE
        --- Recursion happens in here
      END IF;
    ELSE
      RAISE EXCEPTION 'Not able to interpret this selector: %', sub.selector;
    END CASE;
  END LOOP;
  DECLARE
    column_expression text;
  BEGIN
    IF cols > ARRAY[]::text[] THEN
      column_expression := array_to_string(cols, ', ');
    ELSE
      column_expression := format('%I', tab);
    END IF;
    IF pk IS NOT NULL THEN                              -- Implies single result
      q := 'SELECT to_json('  || column_expression || E')\n  ' || q;
    ELSE
      q := 'SELECT json_agg(' || column_expression || E')\n  ' || q;
    END IF;
  END;
  RETURN q;
END
$$ LANGUAGE plpgsql STABLE STRICT;

CREATE FUNCTION parse_many(expr text)
RETURNS TABLE (selector text, predicate text, body text) AS $$
DECLARE
  whitespace text = E' \t\n';
BEGIN
  --- To parse many expressions:
  --- * Parse one expression.
  --- * Consume whitespace.
  --- * Consume a comma if present.
  --- * Consume whitespace.
  --- * Repeat until the input is empty.
  expr := ltrim(expr, whitespace);
  WHILE expr != '' LOOP
    SELECT * FROM graphql.parse_one(expr) INTO selector, predicate, body, expr;
    RETURN NEXT;
    expr := ltrim(expr, whitespace);
    IF substr(expr, 1, 1) = ',' THEN
      expr := substr(expr, 2);
    END IF;
    expr := ltrim(expr, whitespace);
  END LOOP;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION parse_one(expr text,
                          OUT selector text,
                          OUT predicate text,
                          OUT body text,
                          OUT remainder text) AS $$
DECLARE
  label text = '[a-zA-Z_][a-zA-Z0-9_]*';
  selector_re text = '^(' || label || ')' || '([(]([^()]+)[)])?';
  matches text[];
  whitespace text = E' \t\n';
  idx integer = 0;
  nesting integer = 0;
  brackety boolean = FALSE;
  c text;
BEGIN
  --- To parse one expression:
  --- * Consume whitespace.
  --- * Find a selector.
  --- * Consume whitespace.
  --- * Find a left bracket or stop.
  ---   * If there is a left bracket, balance brackets.
  ---   * If there is something else, return.
  expr := ltrim(expr, whitespace);
  matches := regexp_matches(expr, selector_re);
  selector := matches[1];
  predicate := matches[3];
  IF selector IS NULL THEN
    RAISE EXCEPTION 'No selector (in "%")',
                    graphql.excerpt(expr, 1, 50);
  END IF;
  expr := ltrim(regexp_replace(expr, selector_re, ''), whitespace);
  FOREACH c IN ARRAY string_to_array(expr, NULL) LOOP
    idx := idx + 1;
    CASE
    WHEN c = '{' THEN
      nesting := nesting + 1;
      brackety := TRUE;
    WHEN c = '}' AND brackety THEN
      nesting := nesting - 1;
      EXIT WHEN nesting = 0;
    WHEN nesting < 0 THEN
      RAISE EXCEPTION 'Brace nesting error (in "%")',
                      graphql.excerpt(expr, idx, 50);
    ELSE
      EXIT WHEN NOT brackety;
    END CASE;
  END LOOP;
  body := substr(expr, 1, idx);
  remainder := substr(expr, idx+1);
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION excerpt(str text, start integer, length integer)
RETURNS text AS $$
  SELECT substr(regexp_replace(str, '[ \n\t]+', ' ', 'g'), start, length);
$$ LANGUAGE sql IMMUTABLE STRICT;


 /* * * * * * * * * * * * * Table inspection functions * * * * * * * * * * * */

CREATE VIEW pk AS
SELECT attrelid::regclass AS tab,
       array_agg(attname)::name[] AS cols
  FROM pg_attribute
  JOIN pg_index ON (attrelid = indrelid AND attnum = ANY (indkey))
 WHERE indisprimary
 GROUP BY attrelid;

CREATE VIEW cols AS
SELECT attrelid::regclass AS tab,
       attname::name AS col,
       atttypid::regtype AS typ,
       attnum AS num
  FROM pg_attribute
 WHERE attnum > 0
 ORDER BY attrelid, attnum;

CREATE VIEW fk AS
SELECT conrelid::regclass AS tab,
       names.cols,
       confrelid::regclass AS other,
       names.refs
  FROM pg_constraint,
       LATERAL (SELECT array_agg(cols.attname) AS cols,
                       array_agg(refs.attname) AS refs
                  FROM unnest(conkey, confkey) AS _(col, ref),
                       LATERAL (SELECT * FROM pg_attribute
                                 WHERE attrelid = conrelid AND attnum = col)
                            AS cols,
                       LATERAL (SELECT * FROM pg_attribute
                                 WHERE attrelid = confrelid AND attnum = ref)
                            AS refs)
            AS names
 WHERE confrelid != 0;

CREATE FUNCTION ns(tab regclass) RETURNS name AS $$
  SELECT nspname
    FROM pg_class JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
   WHERE pg_class.oid = tab
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION pk(t regclass) RETURNS name[] AS $$
  SELECT cols FROM meta.pk WHERE meta.pk.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION cols(t regclass)
RETURNS TABLE (num smallint, col name, typ regtype) AS $$
  SELECT num, col, typ FROM meta.cols WHERE meta.cols.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION fk(t regclass)
RETURNS TABLE (cols name[], other regclass, refs name[]) AS $$
  SELECT cols, other, refs FROM meta.fk WHERE meta.fk.tab = t;
$$ LANGUAGE sql STABLE STRICT;

COMMIT;
