/*-------------------------------------------------------------------------
 *
 * vtype.h
 *	Vector type
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/vtype/vtype.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTORIZATION_VTYPE_H
#define VECTORIZATION_VTYPE_H

#include "postgres.h"

#include "fmgr.h"
#include "catalog/namespace.h"
#include "executor/tuptable.h"

#include "columnar/vectorization/columnar_vectortupleslot.h"

#ifdef bool
#undef bool
typedef _Bool bool;
#endif

typedef int16 int2;
typedef int32 int4;


#define CANARYSIZE  sizeof(char)
#define VTYPEHEADERSZ (sizeof(Vtype))

#define VDATUMSZ(dim) (sizeof(Datum) * dim)
#define ISNULLSZ(dim) (sizeof(bool) * dim)
#define VTYPESIZE(dim) (VTYPEHEADERSZ + VDATUMSZ(dim) + CANARYSIZE + ISNULLSZ(dim))

#define VTYPE_STRUCTURE(type)  typedef struct Vtype v##type;

#define FUNCTION_BUILD_HEADER(type) v##type* buildv##type(int dim, bool *skip);

#define FUNCTION_IN_HEADER(type, typeoid) extern Datum v##type##in(PG_FUNCTION_ARGS);

#define FUNCTION_OUT_HEADER(type, typeoid) extern Datum v##type##out(PG_FUNCTION_ARGS);

#define TYPE_HEADER(type,oid) \
    VTYPE_STRUCTURE(type) \
	FUNCTION_BUILD_HEADER(type) \
    FUNCTION_IN_HEADER(type, typeoid) \
    FUNCTION_OUT_HEADER(type, typeoid) \

TYPE_HEADER(int2, INT2OID)
TYPE_HEADER(int4, INT4OID)
TYPE_HEADER(int8, INT8OID)
TYPE_HEADER(bool, BOOLOID)
TYPE_HEADER(text, TEXTOID)

// Vector column

typedef struct Vtype
{
	Oid		elemtype;
	int     elemsize;
	bool	elemval;
	int		dim;
	Datum	*values;
	bool    isnull[VECTOR_BATCH_SIZE];
	bool    isDistinct[VECTOR_BATCH_SIZE];
	bool    *skipref;
} Vtype;

extern Vtype* build_vtype(Oid elemtype, int elemsize, int dim, bool *skip);
extern void destroy_vtype(Vtype** vt);

#endif
