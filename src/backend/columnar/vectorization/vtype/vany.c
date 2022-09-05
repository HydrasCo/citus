/*-------------------------------------------------------------------------
 *
 * vany.c
 *
 * $Id*
 * 
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_type.h"

#include "columnar/vectorization/vtype/vany.h"
#include "columnar/vectorization/vtype/vtype.h"

PG_FUNCTION_INFO_V1(vany_in);
PG_FUNCTION_INFO_V1(vany_out);

Datum vany_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vany_in not supported");
}

Datum vany_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vany_out not supported");
}
