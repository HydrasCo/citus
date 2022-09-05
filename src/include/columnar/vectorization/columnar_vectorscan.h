/*-------------------------------------------------------------------------
 *
 * columnar_vectorscan.h
 *	Custom vector scan
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/columnar_vectorscan.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTORIZATION_VECTORSCAN_H
#define VECTORIZATION_VECTORSCAN_H

#include "postgres.h"

#include "nodes/extensible.h"

typedef struct VectorScanState
{
	CustomScanState	css;

	/* Attributes for vectorization */
	ExprContext		*css_RuntimeContext;
	List 			*qual;
} VectorScanState;

const CustomScanMethods * columnar_vectorscan_methods(void);
void columnar_vectorscan_register(void);

#endif