BEGIN;

DROP SCHEMA IF EXISTS graphql CASCADE;
CREATE SCHEMA graphql;

CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

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


SET LOCAL search_path TO graphql, public;
--- However, we still qualify references between functions -- as when `to_sql`
--- calls `parse_many` -- because the search_path will be different when the
--- code is run by the application/user.


 /* * * * * * * * * * * * * Table inspection utilities * * * * * * * * * * * */
 /* These are up here because the types defined by the VIEWs are used further
  * down.
  */

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
                       array_agg(cols.attnum)  AS nums,
                       array_agg(refs.attname) AS refs
                  FROM unnest(conkey, confkey) AS _(col, ref),
                       LATERAL (SELECT * FROM pg_attribute
                                 WHERE attrelid = conrelid AND attnum = col)
                            AS cols,
                       LATERAL (SELECT * FROM pg_attribute
                                 WHERE attrelid = confrelid AND attnum = ref)
                            AS refs)
            AS names
 WHERE confrelid != 0
 ORDER BY (conrelid, names.nums);             -- Returned in column index order

CREATE FUNCTION ns(tab regclass) RETURNS name AS $$
  SELECT nspname
    FROM pg_class JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
   WHERE pg_class.oid = tab
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION pk(t regclass) RETURNS graphql.pk AS $$
  SELECT * FROM graphql.pk WHERE graphql.pk.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION cols(t regclass) RETURNS TABLE graphql.cols AS $$
  SELECT * FROM graphql.cols WHERE graphql.cols.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION fk(t regclass) RETURNS TABLE graphql.fk AS $$
  SELECT * FROM graphql.fk WHERE graphql.fk.tab = t;
$$ LANGUAGE sql STABLE STRICT;


 /* * * * * * * * * * * * * * * Begin main program * * * * * * * * * * * * * */

CREATE FUNCTION run(expr text)
RETURNS json AS $$
DECLARE
  intermediate json;
  result json[] = ARRAY[]::json[];
  n integer = 0;
  q text;
BEGIN
  FOR q IN SELECT graphql.to_sql(expr) LOOP
    n := n + 1;
    BEGIN
      EXECUTE q INTO STRICT intermediate;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN CONTINUE;
    END;
    result := result || intermediate;
  END LOOP;
  --- Maybe there is a better way to approach query cardinality? For example,
  --- by insisting that there be a root query (perhaps with no predicate?) or
  --- returning TABLE (result json).
  IF n = 1 THEN
    RETURN result[1];
  ELSE
    RETURN to_json(result);
  END IF;
END
$$ LANGUAGE plpgsql STABLE STRICT;

CREATE FUNCTION to_sql(expr text)
RETURNS TABLE (query text) AS $$
BEGIN
  RETURN QUERY SELECT graphql.to_sql(selector, predicate, body)
                 FROM graphql.parse_many(expr);
END
$$ LANGUAGE plpgsql STABLE STRICT;


--- The selector is a table; we should do a table lookup.
CREATE FUNCTION to_sql(selector regclass, args hstore, body text)
RETURNS text AS $$
DECLARE
  tab ALIAS FOR selector;
  sub record;
  fk graphql.fk;
  col graphql.cols;
BEGIN
  FOR sub IN SELECT * FROM graphql.parse_many(body) LOOP
    CASE
    WHEN sub.selector IN (SELECT unnest(cols) FROM graphql.fk(tab)) THEN
      --- A column reference that REFERENCES another table.
    WHEN name(sub.selector) IN (SELECT col FROM graphql.cols(tab)) THEN
      --- A simple column reference.
      SELECT * INTO STRICT col FROM graphql.cols(tab)
       WHERE col = name(sub.selector);
      graphql.to_sql(col, sub.predicate, sub.body);
    WHEN tab IN (SELECT other FROM graphql.fk(regclass(sub.selector))) THEN
      --- A reference to a table that REFERENCES this table.
      SELECT * INTO STRICT fk FROM graphql.fk(regclass(sub.selector))
       WHERE other = graphql.to_sql.tab
       LIMIT 1; -- Rely on ordering in VIEW to get first one in column order.
      graphql.to_sql(fk, sub.predicate, sub.body);
    ELSE
      RAISE EXCEPTION 'We are only able to branch to a column or a JOIN table '
                      'when the base selector is a table';
    END CASE;
  END LOOP;
END
$$ LANGUAGE plpgsql STABLE;

--- The selector involves a foreign key and perhaps a JOIN table; we should do
--- a JOIN.
CREATE FUNCTION to_sql(selector graphql.fk, args hstore, body text)
RETURNS text AS $$
BEGIN
--- TODO
END
$$ LANGUAGE plpgsql STABLE;

--- The selector is a column.
CREATE FUNCTION to_sql(selector graphql.cols, args hstore, body text)
RETURNS text AS $$
BEGIN
--- TODO
END
$$ LANGUAGE plpgsql STABLE;

--- The selector is a lookup key in an `hstore`, `jsonb` or `json` column.
CREATE FUNCTION to_sql(selector text, args hstore, body text)
RETURNS text AS $$
BEGIN
--- TODO
END
$$ LANGUAGE plpgsql STABLE;

--- The selector is an actual stored procedure.
CREATE FUNCTION to_sql(selector regprocedure, args hstore, body text)
RETURNS text AS $$
BEGIN
--- TODO
END
$$ LANGUAGE plpgsql STABLE;


--- The as_label interface returns an escaped identifier for something of the
--- given type, for use as a key in the returned GraphQL data.

--- For tables, we want just the tablename; since there is no concept of
--- schema-qualified names in GraphQL.
CREATE FUNCTION as_label(selector regclass) RETURNS text AS $$
  SELECT format('%I', relname)
    FROM LATERAL (SELECT * FROM pg_class WHERE oid = 'pg_type'::regclass) AS _;
$$ LANGUAGE sql STABLE STRICT;

--- We look for the table containing the REFERENCE and use its name.
CREATE FUNCTION as_label(selector graphql.fk) RETURNS text AS $$
  SELECT graphql.as_label(selector.tab);
$$ LANGUAGE sql STABLE STRICT;

--- We escape the name of the column.
CREATE FUNCTION as_label(selector graphql.cols) RETURNS text AS $$
  SELECT format('%I', selector.col)
$$ LANGUAGE sql STABLE STRICT;

--- A text selector is likely a lookup key of some kind. It must be escaped.
CREATE FUNCTION as_label(selector text) RETURNS text AS $$
  SELECT format('%I', selector)
$$ LANGUAGE sql STABLE STRICT;


--- Base case (and entry point): looking up a row from a table.
CREATE FUNCTION to_sql(selector regclass,
                       predicate text,
                       body text,
                       label name DEFAULT NULL)
RETURNS text AS $$
DECLARE
  q text = '';
  tab regclass = selector;                                       -- For clarity
  cols text[] = ARRAY[]::text[];
  col name;
  sub record;
  pk text[] = NULL;
  fks graphql.fk[];
  subselects text[] = ARRAY[]::text[];
  predicates text[] = ARRAY[]::text[];
BEGIN
  body := btrim(body, '{}');
  IF predicate IS NOT NULL THEN
    SELECT array_agg(_) INTO STRICT pk
      FROM jsonb_array_elements_text(jsonb('['||predicate||']')) AS __(_);
    predicates := predicates
               || graphql.format_comparison(tab, graphql.pk(tab), pk);
  END IF;
  FOR sub IN SELECT * FROM graphql.parse_many(body) LOOP
    IF sub.predicate IS NOT NULL THEN
      RAISE EXCEPTION 'Unhandled nested selector %(%)',
                      sub.selector, sub.predicate;
    END IF;
    SELECT cols.col INTO col
      FROM graphql.cols(tab) WHERE cols.col = sub.selector;
    CASE
    WHEN FOUND AND sub.body IS NULL THEN           -- A simple column reference
      SELECT array_agg(fk) INTO STRICT fks
        FROM graphql.fk(tab)
       WHERE cardinality(fk.cols) = 1 AND fk.cols[1] = col;
      IF cardinality(fks) > 0 THEN
        IF cardinality(fks) > 1 THEN
          RAISE EXCEPTION 'More than one candidate foreign keys for %(%)',
                          tab, col;
        END IF;
        subselects := subselects
                   || format(E'SELECT to_json(%1$s) AS %4$I FROM %1$s\n'
                              ' WHERE %1$s.%2$I = %3$s.%4$I',
                             fks[1].other, fks[1].refs[1], tab, col);
        cols := cols || format('%I.%I', 'sub/'||cardinality(subselects), col);
      ELSE
        cols := cols || format('%s.%I', tab, col);
      END IF;
    WHEN FOUND AND sub.body IS NOT NULL THEN             -- Index into a column
      subselects := subselects || graphql.to_sql(sub.selector,
                                                 sub.predicate,
                                                 sub.body,
                                                 tab);
      cols := cols || format('%I.%I', 'sub/'||cardinality(subselects), col);
    WHEN NOT FOUND THEN             -- It might be a reference to another table
      subselects := subselects || graphql.to_sql(regclass(sub.selector),
                                                 sub.predicate,
                                                 sub.body,
                                                 tab,
                                                 pk);
      cols := cols
           || format('%I.%I', 'sub/'||cardinality(subselects), sub.selector);
    ELSE
      RAISE EXCEPTION 'Not able to interpret this selector: %', sub.selector;
    END CASE;
  END LOOP;
  DECLARE
    column_expression text;
  BEGIN
    IF cols > ARRAY[]::text[] THEN
      --- We want a temporary record type to to pass to json_agg or to_json as
      --- a single parameter so that column names are preserved. So we select
      --- all the columns into a subselect with LATERAL and then reference the
      --- subselect. This subselect should always be the last one in the
      --- sequence, since it needs to reference "columns" created in the other
      --- subselects.
      subselects := subselects
                 || format('SELECT %s', array_to_string(cols, ', '));
      column_expression := format('%I', 'sub/'||cardinality(subselects));
    ELSE
      column_expression := format('%s', tab);
    END IF;
    IF pk IS NOT NULL THEN                             -- Implies single result
      q := 'SELECT to_json('  || column_expression || ')' || q;
    ELSE
      q := 'SELECT json_agg(' || column_expression || ')' || q;
    END IF;
    IF label IS NOT NULL THEN
      q := q || format(' AS %I', label);
    ELSE
      q := q || format(' AS %s', tab);
    END IF;
  END;
  q := q || format(E'\n  FROM %s', tab);
  FOR i IN 1..cardinality(subselects) LOOP
    q := q || array_to_string(ARRAY[
                ',',
                graphql.indent(7, 'LATERAL ('), -- 7 to line up with SELECT ...
                graphql.indent(9, subselects[i]),    -- 9 to be 2 under LATERAL
                graphql.indent(7, ') AS ' || format('%I', 'sub/'||i))
              ], E'\n');
    --- TODO: Find an "indented text" abstraction so we don't split and
    ---       recombine the same lines so many times.
  END LOOP;
  FOR i IN 1..cardinality(predicates) LOOP
    IF i = 1 THEN
      q := q || E'\n WHERE (' || predicates[i] || ')';
    ELSE
      q := q || E'\n   AND (' || predicates[i] || ')';
    END IF;
  END LOOP;
  RETURN q;
END
$$ LANGUAGE plpgsql STABLE;

--- Handling fancy columns: json, jsonb and hstore
CREATE FUNCTION to_sql(selector text, predicate text, body text, tab regclass)
RETURNS text AS $$
DECLARE
  q text = '';
  col name;
  typ regtype;
  sub record;
  lookups text[] = ARRAY[]::text[];
  labels text[] = ARRAY[]::text[];
BEGIN
  SELECT cols.col, cols.typ INTO col, typ
    FROM graphql.cols(tab) WHERE cols.col = selector;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Did not find column % on table %', col, tab;
  END IF;
  FOR sub IN SELECT * FROM graphql.parse_many(body) LOOP
    IF sub.predicate IS NOT NULL THEN
      RAISE EXCEPTION 'Not able to handle predicates when following lookups '
                      'into columns (for field % under %.%)',
                      sub.selector, tab, col;
    END IF;
    CASE typ
    WHEN regtype('jsonb'), regtype('json') THEN
      IF sub.body IS NOT NULL THEN                 -- TODO: Nested JSON lookups
        RAISE EXCEPTION 'Nested JSON lookup is as yet unimplemented';
      END IF;
      lookups := lookups || format('%I->%L', selector, sub.selector);
    WHEN regtype('hstore') THEN
      IF sub.body IS NOT NULL THEN
        RAISE EXCEPTION 'No fields below this level (column % is hstore)',
                        tab, col;
      END IF;
      lookups := lookups || format('%I->%L', selector, sub.selector);
    ELSE
      --- Treat it as a field lookup in a nested record (this could also end up
      --- being a function call, by the way).
      lookups := lookups || format('%I.%I', selector, sub.selector);
    END CASE;
    labels := labels || format('%I', sub.selector);
  END LOOP;
  q := format(E'SELECT to_json(_) AS %I\n'
               '  FROM (VALUES (%s)) AS _(%s)',
              col,
              array_to_string(lookups, ', '),
              array_to_string(labels, ', '));
  RETURN q;
END
$$ LANGUAGE plpgsql STABLE;

--- For tables with foreign keys that point at the target table. Mutually
--- recursive with the base case.
CREATE FUNCTION to_sql(selector regclass,
                       predicate text,
                       body text,
                       tab regclass,
                       keys text[])
RETURNS text AS $$
DECLARE
  q text = '';
  txts text[];
  ikey record;                    -- Key which REFERENCEs `tab` from `selector`
  --- If `selector` is a JOIN table, then `okey` is used to store a REFERENCE
  --- to the table with the actual data.
  okey record;
BEGIN
  BEGIN
    SELECT * INTO STRICT ikey     -- Find the first foreign key in column order
      FROM graphql.fk(selector) WHERE fk.other = tab LIMIT 1;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE EXCEPTION 'No REFERENCE to table % from table %', tab, selector;
  END;
  PERFORM * FROM graphql.cols(selector)
    WHERE cols.col NOT IN (SELECT unnest(cols) FROM graphql.fk(selector))
      AND cols.typ NOT IN (regtype('timestamp'), regtype('timestamptz'));
  --- If:
  --- * Thare are two and only two foreign keys for the other table, and
  --- * All the columns of the table participate in one or the other
  ---   foreign key, or are timestamps, then
  --- * We can treat the table as a JOIN table and follow the keys.
  --- Otherwise:
  --- * We use the existence of the foreign key to look up the record in
  ---   the table that JOINs with us.
  IF NOT FOUND AND (SELECT count(1) FROM graphql.fk(selector)) = 2 THEN
    SELECT * INTO STRICT okey FROM graphql.fk(selector) WHERE fk != ikey;
    q := graphql.to_sql(okey.other, NULL, body, name(selector));
    --- Split at the first LATERAL and put the JOIN behind it, if there is a
    --- LATERAL.
    SELECT regexp_matches(q, '^(.+)(,[ \n\t]+LATERAL)(.+)$') INTO txts;
    IF FOUND THEN
      q := txts[1]
        || E'\n  '
        || graphql.format_join(okey.other, okey.refs, selector, okey.cols)
        || txts[2]
        || txts[3];
    ELSE
      q := q
        || E'\n  '
        || graphql.format_join(okey.other, okey.refs, selector, okey.cols);
    END IF;
  ELSE
    q := graphql.to_sql(selector, NULL, body, name(selector));
  END IF;
  q := q || E'\n WHERE '
         || graphql.format_comparison(selector, ikey.cols, keys);
  RETURN q;
END
$$ LANGUAGE plpgsql STABLE;

CREATE FUNCTION parse_many(expr text)
RETURNS TABLE (selector text, predicate text, body text) AS $$
DECLARE
  whitespace_and_commas text = E'^[ \t\n,]*';
BEGIN
  --- To parse many expressions:
  --- * Parse one expression.
  --- * Consume whitespace.
  --- * Consume a comma if present.
  --- * Consume whitespace.
  --- * Repeat until the input is empty.
  expr := regexp_replace(expr, whitespace_and_commas, '');
  WHILE expr != '' LOOP
    SELECT * FROM graphql.parse_one(expr) INTO selector, predicate, body, expr;
    RETURN NEXT;
    expr := regexp_replace(expr, whitespace_and_commas, '');
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
  IF brackety THEN
    body := substr(expr, 1, idx);
  END IF;
  remainder := substr(expr, idx+1);
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


 /* * * * * * * * * * * * * * * * Text utilities * * * * * * * * * * * * * * */

CREATE FUNCTION excerpt(str text, start integer, length integer)
RETURNS text AS $$
  SELECT substr(regexp_replace(str, '[ \n\t]+', ' ', 'g'), start, length);
$$ LANGUAGE sql IMMUTABLE STRICT;

CREATE FUNCTION indent(level integer, str text)
RETURNS text AS $$
  SELECT array_to_string(array_agg(s), E'\n')
    FROM unnest(string_to_array(str, E'\n')) AS _(ln),
         LATERAL (SELECT repeat(' ', level))
              AS spacer(spacer),
         LATERAL (SELECT CASE ln WHEN '' THEN ln ELSE spacer || ln END)
              AS indented(s)
$$ LANGUAGE sql IMMUTABLE STRICT;

CREATE FUNCTION format_comparison(x regclass, xs name[], y regclass, ys name[])
RETURNS text AS $$
  WITH xs(col) AS (SELECT format('%s.%I', x, col) FROM unnest(xs) AS _(col)),
       ys(col) AS (SELECT format('%s.%I', y, col) FROM unnest(ys) AS _(col))
  SELECT format('(%s) = (%s)',
                array_to_string((SELECT array_agg(col) FROM xs), ', '),
                array_to_string((SELECT array_agg(col) FROM ys), ', '))
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION format_comparison(x name, xs name[], y regclass, ys name[])
RETURNS text AS $$
  WITH xs(col) AS (SELECT format('%I.%I', x, col) FROM unnest(xs) AS _(col)),
       ys(col) AS (SELECT format('%s.%I', y, col) FROM unnest(ys) AS _(col))
  SELECT format('(%s) = (%s)',
                array_to_string((SELECT array_agg(col) FROM xs), ', '),
                array_to_string((SELECT array_agg(col) FROM ys), ', '))
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION format_comparison(x regclass, xs name[], y name, ys name[])
RETURNS text AS $$
  WITH xs(col) AS (SELECT format('%s.%I', x, col) FROM unnest(xs) AS _(col)),
       ys(col) AS (SELECT format('%I.%I', y, col) FROM unnest(ys) AS _(col))
  SELECT format('(%s) = (%s)',
                array_to_string((SELECT array_agg(col) FROM xs), ', '),
                array_to_string((SELECT array_agg(col) FROM ys), ', '))
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION format_comparison(x name, xs name[], y name, ys name[])
RETURNS text AS $$
  WITH xs(col) AS (SELECT format('%I.%I', x, col) FROM unnest(xs) AS _(col)),
       ys(col) AS (SELECT format('%I.%I', y, col) FROM unnest(ys) AS _(col))
  SELECT format('(%s) = (%s)',
                array_to_string((SELECT array_agg(col) FROM xs), ', '),
                array_to_string((SELECT array_agg(col) FROM ys), ', '))
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION format_comparison(x regclass, xs name[], ys text[])
RETURNS text AS $$
  WITH xs(col) AS (SELECT format('%s.%I', x, col) FROM unnest(xs) AS _(col)),
       named(col, txt) AS (SELECT * FROM unnest(xs, ys)),
       casted(val) AS (SELECT format('CAST(%L AS %s)', txt, typ)
                         FROM named JOIN graphql.cols(x) USING (col))
  SELECT format('(%s) = (%s)',
                array_to_string((SELECT array_agg(col) FROM xs), ', '),
                array_to_string((SELECT array_agg(val) FROM casted), ', '))
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION format_join(tab regclass,
                            cols name[],
                            other regclass,
                            refs name[],
                            label name DEFAULT NULL)
RETURNS text AS $$
  SELECT CASE WHEN label IS NULL THEN
           format('JOIN %s ON (%s)',
                  other,
                  graphql.format_comparison(tab, cols, other, refs))
         ELSE
           format('JOIN %s AS %I ON (%s)',
                  other,
                  label,
                  graphql.format_comparison(tab, cols, label, refs))
         END
$$ LANGUAGE sql STABLE;

END;
