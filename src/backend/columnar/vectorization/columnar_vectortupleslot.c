/*-------------------------------------------------------------------------
 *
 * columnar_vectortupleslot.c
 *
 * $Id*
 * 
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/sysattr.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "utils/expandeddatum.h"

#include "columnar/vectorization/columnar_vectortupleslot.h"
#include "columnar/vectorization/vtype/vtype.h"


static TupleTableSlot *
VectorMakeSlot(TupleDesc tupleDesc)
{
	TupleTableSlot	*slot;
	VectorTupleSlot	*vslot;
	int				i;
	Oid				typid;
	int16			typlen;
	Vtype			*column;
	static TupleTableSlotOps tts_ops;

	tts_ops = TTSOpsVirtual;
	tts_ops.base_slot_size = sizeof(VectorTupleSlot);

	slot = MakeTupleTableSlot(tupleDesc, &tts_ops);

	/* vectorized fields */
	vslot = (VectorTupleSlot*)slot;
	
	/* all tuples should be skipped in initialization */
	memset(vslot->skip, true, sizeof(vslot->skip));

	for (i = 0; i < tupleDesc->natts; i++)
	{
		typid = TupleDescAttr(tupleDesc, i)->atttypid;
		typlen = TupleDescAttr(tupleDesc, i)->attlen == -1 ?  sizeof(Datum) : TupleDescAttr(tupleDesc, i)->attlen;
		
		column = build_vtype(typid, typlen, VECTOR_BATCH_SIZE, vslot->skip);
		
		column->dim = 0;
		column->elemval = TupleDescAttr(tupleDesc, i)->attlen == -1  ? false : true;

		vslot->tts.tts_values[i] = PointerGetDatum(column);
		vslot->tts.tts_isnull[i] = false;
	}

	return slot;
}


static TupleTableSlot *
VectorAllocSlot(List **tupleTable, TupleDesc desc)
{
	TupleTableSlot *slot = VectorMakeSlot(desc);

	*tupleTable = lappend(*tupleTable, slot);

	return slot;
}


void
vector_init_scan_slot(EState *estate, ScanState *scanstate, TupleDesc tupledesc)
{
	scanstate->ss_ScanTupleSlot = VectorAllocSlot(&estate->es_tupleTable, tupledesc);
	scanstate->ps.scandesc = tupledesc;
	scanstate->ps.scanopsfixed = tupledesc != NULL;
	scanstate->ps.scanops = scanstate->ss_ScanTupleSlot->tts_ops;
	scanstate->ps.scanopsset = true;
}


void
vector_init_result_slot(EState *estate, PlanState *planstate)
{
	planstate->ps_ResultTupleSlot = 
		VectorAllocSlot(&estate->es_tupleTable, planstate->ps_ResultTupleDesc);
}


TupleTableSlot *
vector_init_extra_slot(EState *estate, TupleDesc tupledesc)
{
	return VectorAllocSlot(&estate->es_tupleTable, tupledesc);
}


TupleTableSlot *
vector_clear_slot(TupleTableSlot *slot)
{
	int				i;
	Vtype			*column;
	VectorTupleSlot *vslot;

	Assert(slot != NULL);

	vslot = (VectorTupleSlot *)slot;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;

	vslot->dim = 0;

	for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		column = (Vtype *)DatumGetPointer(slot->tts_values[i]);
		column->dim = 0;
	}

	memset(vslot->skip, true, sizeof(vslot->skip));

	return slot;
}