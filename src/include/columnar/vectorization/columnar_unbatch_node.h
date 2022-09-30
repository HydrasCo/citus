/*-------------------------------------------------------------------------
 *
 * columnar_unbatch_node.h
 *	Custom scan method for converting vectorized tuple table slots
 *	to single tuple table slots for output
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/columnar_unbatch_node.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTORIZATION_UNBATCH_NODE_H
#define VECTORIZATION_UNBATCH_NODE_H

#include "nodes/plannodes.h"

extern Plan * columnar_add_unbatch_plan(Plan *node);
extern CustomScan * columnar_create_unbatch_node(void);
extern void columnar_unbatch_node_register(void);

#endif
