-- Known grammar limitation (AndreasMaierDe/tree-sitter-plsql @ 28aebef):
-- CREATE TYPE ... AS OBJECT currently produces ERROR nodes. Capture this
-- minimal repro so a future grammar upgrade can clear the skip.
CREATE OR REPLACE TYPE address_t AS OBJECT (
  street VARCHAR2(100),
  city   VARCHAR2(50)
);
/
