/*-------------------------------------------------------------------------
 *
 * columnar_agg_node.h
 *	Custom scan method for aggregation
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/columnar_agg_node.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef VECTORIZATION_AGG_NODE_H
#define VECTORIZATION_AGG_NODE_H

#include "postgres.h"

#include "server/nodes/execnodes.h"

/*
 * VectorAggState - state object of vectoragg on executor.
 */
typedef struct VectorAggState
{
	CustomScanState	css;

	/* Attributes for vectorized aggregate */
	AggState		*aggstate;
	TupleTableSlot	*vectorResultSlot;
	MemoryContext	vectorAggStateContext;
} VectorAggState;

typedef struct AggTransHashState
{
    int transno;
    HTAB *hash;
} AggTransHashState;

extern CustomScan *make_vectoraggscan_customscan(void);
extern void columnar_agg_node_register(void);

#endif