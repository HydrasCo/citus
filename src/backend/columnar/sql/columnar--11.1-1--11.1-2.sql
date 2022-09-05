-- columnar--11.1-1--11.2-1.sql

#include "udfs/alter_table_set_access_method/11.1-2.sql"

-- DROP AGG functions

DROP AGGREGATE IF EXISTS count(vany);

-- DROP FUNCTIONS

DROP FUNCTION IF EXISTS vint8inc(int8, vany);

-- DROP TYPES

DROP TYPE IF EXISTS vany  cascade;
DROP TYPE IF EXISTS vint2 cascade;
DROP TYPE IF EXISTS vint4 cascade;
DROP TYPE IF EXISTS vint8 cascade;
DROP TYPE IF EXISTS vbool cascade;

-- create vectorized types

CREATE TYPE vany;
CREATE FUNCTION vany_in(cstring) RETURNS vany AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION vany_out(vany) RETURNS cstring AS'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE vany ( INPUT = vany_in, OUTPUT = vany_out, storage=plain );

CREATE TYPE vint2;
CREATE FUNCTION vint2in(cstring) RETURNS vint2 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION vint2out(vint2) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE vint2 ( INPUT = vint2in, OUTPUT = vint2out, storage = plain, internallength = 2, alignment = int2, PASSEDBYVALUE );

CREATE TYPE vint4;
CREATE FUNCTION vint4in(cstring) RETURNS vint4 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION vint4out(vint4) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE vint4 ( INPUT = vint4in, OUTPUT = vint4out, storage = plain, internallength = 4, alignment = int4, PASSEDBYVALUE );

CREATE TYPE vint8;
CREATE FUNCTION vint8in(cstring) RETURNS vint8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION vint8out(vint8) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE vint8 ( INPUT = vint8in, OUTPUT = vint8out, storage = plain, internallength = 8, alignment = double, PASSEDBYVALUE);

CREATE TYPE vbool;
CREATE FUNCTION vboolin(cstring) RETURNS vbool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION vboolout(vbool) RETURNS cstring AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE vbool ( INPUT = vboolin, OUTPUT = vboolout, element = bool, storage =plain, SUBSCRIPT = raw_array_subscript_handler );
  
-- create operators for the vectorized types

    -- vint4

CREATE FUNCTION vint4int4lt(vint4, int4) RETURNS vbool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE OPERATOR < ( leftarg = vint4, rightarg = int4, procedure = vint4int4lt, commutator = >= );

CREATE FUNCTION vint4vint4pl(vint4, vint4) RETURNS vint4 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE OPERATOR + ( leftarg = vint4, rightarg = vint4, procedure = vint4vint4pl, commutator = - );

    -- vbool
    
CREATE FUNCTION vbool_and_vbool(vbool, vbool) RETURNS vbool AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT;
CREATE OPERATOR & ( leftarg = vbool, rightarg = vbool, procedure = vbool_and_vbool);

-- create aggregate

CREATE FUNCTION vint8inc(int8, vany) RETURNS int8 AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE;
CREATE AGGREGATE count(vany) ( sfunc = vint8inc, stype = int8 );