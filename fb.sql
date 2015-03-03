CREATE SCHEMA fb;
SET search_path TO fb,"$user",public;
CREATE EXTENSION "uuid-ossp";


CREATE TABLE "user" (
  id            uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  created       timestamptz NOT NULL DEFAULT now(),
  full_name     text NOT NULL DEFAULT ''
);


CREATE TABLE post (
  id            uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  created       timestamptz NOT NULL DEFAULT now(),
  content       text NOT NULL DEFAULT '',
  "user"        uuid REFERENCES "user" NOT NULL
);


CREATE TABLE friendship (
  first         uuid REFERENCES "user" NOT NULL,
  second        uuid REFERENCES "user" NOT NULL,
  created       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (first, second)
);

CREATE FUNCTION check_friendship_symmetry() RETURNS TRIGGER AS $$
DECLARE
  link friendship;
BEGIN
  SELECT * INTO link FROM friendship
   WHERE second = NEW.first AND first = NEW.second;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Friendships must be INSERTed as pairs.';
  END IF;
  RETURN NEW;
END
$$ LANGUAGE 'plpgsql'
   SET search_path FROM CURRENT;

CREATE CONSTRAINT TRIGGER friendship_symmetry
AFTER INSERT ON friendship
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE PROCEDURE check_friendship_symmetry();




/*

INSERT INTO "user" (id, full_name) VALUES
  ('606fa027-a577-4018-952e-3c8469372829', 'Curly'),
  ('f3411edc-e1d0-452a-bc19-b42c0d5a0e36', 'Larry'),
  ('05ed1b59-090e-40af-8f0e-68a1129b55b4', 'Mo');

--- Should succeed because friendships are symmetrical.
INSERT INTO friendship (first, second) VALUES                      
  ('f3411edc-e1d0-452a-bc19-b42c0d5a0e36',
   '05ed1b59-090e-40af-8f0e-68a1129b55b4'),
  ('05ed1b59-090e-40af-8f0e-68a1129b55b4',
   'f3411edc-e1d0-452a-bc19-b42c0d5a0e36');

--- Should error out.
INSERT INTO friendship (first, second) VALUES
  ('f3411edc-e1d0-452a-bc19-b42c0d5a0e36',
   '606fa027-a577-4018-952e-3c8469372829');

 */
