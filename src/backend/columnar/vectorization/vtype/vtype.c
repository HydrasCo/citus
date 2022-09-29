/*-------------------------------------------------------------------------
 *
 * vtype.c
 *
 * $Id*
 * 
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/date.h"

#include "columnar/vectorization/columnar_vectortupleslot.h"
#include "columnar/vectorization/vtype/vtype.h"

#define MAX_NUM_LEN 64

Vtype* build_vtype(Oid elemtype, int elemsize, int dim, bool *skip)
{
	Vtype *res;
	res = palloc0(sizeof(Vtype));
	res->dim = dim;
	res->elemtype = elemtype;
	res->elemsize = elemsize;
	res->values = palloc0(elemsize * dim);
	res->skipref = skip;
	return res;
}

void destroy_vtype(Vtype** vt)
{
	pfree((*vt));
	*vt = NULL;
}

#define _FUNCTION_BUILD(type, typelen ,typeoid) \
v##type* buildv##type(int dim, bool *skip) \
{ \
	return build_vtype(typeoid, typelen, dim, skip); \
}

/*
 * IN function for the abstract data types
 * e.g. Datum vint2in(PG_FUNCTION_ARGS)
 */
#define _FUNCTION_IN(type, typelen, fname, typeoid) \
PG_FUNCTION_INFO_V1(v##fname##in); \
Datum \
v##fname##in(PG_FUNCTION_ARGS) \
{ \
	char *intString = PG_GETARG_CSTRING(0); \
	Vtype *res = NULL; \
	char tempstr[MAX_NUM_LEN] = {0}; \
	int n = 0; \
	res = build_vtype(typeoid, typelen, VECTOR_BATCH_SIZE, NULL);\
	for (n = 0; *intString && n < VECTOR_BATCH_SIZE; n++) \
	{ \
		char *start = NULL;\
		while (*intString && isspace((unsigned char) *intString)) \
			intString++; \
		if (*intString == '\0') \
			break; \
		start = intString; \
		while ((*intString && !isspace((unsigned char) *intString)) && *intString != '\0') \
			intString++; \
		Assert(intString - start < MAX_NUM_LEN); \
		strncpy(tempstr, start, intString - start); \
		tempstr[intString - start] = 0; \
		res->values[n] = DirectFunctionCall1(fname##in, CStringGetDatum(tempstr)); \
		while (*intString && !isspace((unsigned char) *intString)) \
			intString++; \
	} \
	while (*intString && isspace((unsigned char) *intString)) \
		intString++; \
	if (*intString) \
		ereport(ERROR, \
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
				errmsg("int2vector has too many elements"))); \
	res->elemtype = typeoid; \
	res->elemsize = typelen; \
	res->dim = n; \
	SET_VARSIZE(res, VTYPESIZE(n)); \
	PG_RETURN_POINTER(res); \
}

/*
 * OUT function for the abstract data types
 * e.g. Datum vint2out(PG_FUNCTION_ARGS)
 */
#define _FUNCTION_OUT(type, fname, typeoid) \
PG_FUNCTION_INFO_V1(v##fname##out); \
Datum \
v##fname##out(PG_FUNCTION_ARGS) \
{ \
	Vtype * arg1 = (v##type *) PG_GETARG_POINTER(0); \
	int len = arg1->dim; \
	int i = 0; \
	char *rp; \
	char *result; \
	rp = result = (char *) palloc0(len * MAX_NUM_LEN + 1); \
	for (i = 0; i < len; i++) \
	{ \
		if (i != 0) \
			*rp++ = ' '; \
		strcat(rp, DatumGetCString(DirectFunctionCall1(fname##out, arg1->values[i])));\
		while (*++rp != '\0'); \
	} \
	*rp = '\0'; \
	PG_RETURN_CSTRING(result); \
}

#define FUNCTION_BUILD(type, typelen, fname, typeoid) \
	_FUNCTION_BUILD(type, typelen ,typeoid) \
	_FUNCTION_IN(type,typelen, fname, typeoid) \
	_FUNCTION_OUT(type, fname, typeoid)

FUNCTION_BUILD(int2, 2, int2, INT2OID)
FUNCTION_BUILD(int4, 4, int4, INT4OID)
FUNCTION_BUILD(int8, 8, int8, INT8OID)
FUNCTION_BUILD(bool, 1, bool, BOOLOID)
FUNCTION_BUILD(text, -1, text, TEXTOID)
