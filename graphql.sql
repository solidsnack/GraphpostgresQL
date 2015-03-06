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

CREATE FUNCTION graphql.to_sql(expr text)
RETURNS TABLE (query text) AS $$
BEGIN
  RETURN QUERY SELECT graphql.to_sql(selector, predicate, body)
                 FROM graphql.parse_many(expr);
END
$$ LANGUAGE plpgsql STABLE STRICT;

CREATE FUNCTION graphql.to_sql(selector text, predicate text, body text)
RETURNS text AS $$
DECLARE
  q text;
  tab regclass = selector::regclass;       -- Without a parent, we need a table
  cols text[];
  sub record;
  pk text;
BEGIN
  q := 'FROM ' || tab;                              -- Regclass is auto-escaped
  body := substr(body, 2, length(body)-2);
  IF predicate IS NOT NULL THEN
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
    --- TODO: Handle nested lookup into JSON, HStore, RECORD
    --- TODO: Introduce foreign key magicks
    cols := cols || format('%I', sub.selector);
  END LOOP;
  IF cols > ARRAY[]::text[] THEN
    q := 'SELECT ' || array_to_string(cols, ', ') || E' \n  ' || q;
  ELSE
    q := 'SELECT *' || E' \n  ' || q;
  END IF;
  RETURN q;
END
$$ LANGUAGE plpgsql STABLE STRICT;

CREATE FUNCTION graphql.parse_many(expr text)
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

CREATE FUNCTION graphql.parse_one(expr text,
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

CREATE FUNCTION graphql.pk(tab regclass)
RETURNS TABLE (col name, typ regtype) AS $$
  SELECT attname, atttypid::regtype
    FROM pg_index JOIN pg_attribute ON (attnum = ANY (indkey))
   WHERE indrelid = tab AND indisprimary AND attrelid = tab AND attnum > 0
   ORDER BY attnum
$$ LANGUAGE sql STABLE STRICT;
--- NB: For SELECTs, it would be okay just to return the column numbers. One
---     could skip the JOIN with pg_attribute, resulting in a faster query.

CREATE FUNCTION graphql.excerpt(str text, start integer, length integer)
RETURNS text AS $$
  SELECT substr(regexp_replace(str, '[ \n\t]+', ' ', 'g'), start, length);
$$ LANGUAGE sql IMMUTABLE STRICT;
