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
