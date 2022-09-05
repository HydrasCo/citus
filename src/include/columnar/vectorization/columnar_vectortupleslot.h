/*-------------------------------------------------------------------------
 *
 * columnar_vectortupleslot.h
 *	Vector tuple table slot support
 *
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/columnar_vectortupleslot.c
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_TUPLESLOT_H
#define VECTOR_TUPLESLOT_H

#include "postgres.h"

#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"

#define VECTOR_BATCH_SIZE 16384 // 16384 // 8192

typedef struct VectorTupleSlot
{
	TupleTableSlot	tts;
	/* How many tuples does this slot contain */ 
	int32			dim;
	/* Skip array to represent filtered tuples */
	bool			skip[VECTOR_BATCH_SIZE];
} VectorTupleSlot;

extern void vector_init_scan_slot(EState *estate, ScanState *scanstate,
								  TupleDesc tupledesc);
extern void vector_init_result_slot(EState *estate, PlanState *planstate);
extern TupleTableSlot * vector_init_extra_slot(EState *estate, TupleDesc tupledesc);
extern TupleTableSlot * vector_clear_slot(TupleTableSlot *slot);

#endif
