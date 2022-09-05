/*-------------------------------------------------------------------------
 *
 * utils.h
 *	Vectorization utility functions
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTORIZATION_UTILS_H
#define VECTORIZATION_UTILS_H

#include "postgres.h"

#include "access/tupdesc.h"

extern Oid  get_vector_type(Oid ntype);
extern Oid  get_normal_type(Oid vtype);

#endif
