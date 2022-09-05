/*-------------------------------------------------------------------------
 *
 * unbatch_node.c
 *
 * $Id*
 * 
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"
#include "executor/nodeCustom.h"
#include "nodes/extensible.h"

#include "columnar/utils/listutils.h"
#include "columnar/vectorization/columnar_unbatch_node.h"
#include "columnar/vectorization/vtype/vtype.h"
#include "columnar/vectorization/utils.h"
#include "columnar/vectorization/columnar_vectortupleslot.h"


typedef struct UnbatchState
{
	CustomScanState	css;
	TupleTableSlot *ps_ResultTupleSlot; 
	int				resultVectorIndex;
	int				*columnRowOffset;
} UnbatchState;

static Node *CreateUnbatchState(CustomScan *custom_plan);
static TupleTableSlot *FetchRowFromBatch(UnbatchState *ubs);
static bool ReadNextVectorSlot(UnbatchState *ubs);

static void BeginUnbatch(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *ExecUnbatch(CustomScanState *node);
static void EndUnbatch(CustomScanState *node);

static CustomScanMethods UnbatchNodeMethods = {
	"UnBatchNode",		/* CustomName */
	CreateUnbatchState,	/* CreateCustomScanState */
};

static CustomExecMethods UnbatchNodeExecMethods = {
	"UnBatchNode",		/* CustomName */
	BeginUnbatch,		/* BeginCustomScan */
	ExecUnbatch,		/* ExecCustomScan */
	EndUnbatch,			/* EndCustomScan */
	NULL,				/* ReScanCustomScan */
	NULL,				/* MarkPosCustomScan */
	NULL,				/* RestrPosCustomScan */
	NULL,				/* EstimateDSMCustomScan */
	NULL,				/* InitializeDSMCustomScan */
	NULL,				/* InitializeWorkerCustomScan */
	NULL,				/* ExplainCustomScan */
};


static void
BeginUnbatch(CustomScanState *node, EState *estate, int eflags)
{
	UnbatchState *ubs = (UnbatchState*) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	TupleDesc tupdesc;

	outerPlanState(ubs) = ExecInitNode(outerPlan(cscan), estate, eflags);

	{
		node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor = 
			CreateTupleDescCopy(outerPlanState(ubs)->ps_ResultTupleSlot->tts_tupleDescriptor);

		tupdesc = node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;

		// Convert outerPlanState tupleDesc types to 'normal' for output result slot
		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr =  TupleDescAttr(tupdesc, i);
			Oid typid = get_normal_type(attr->atttypid);
			if (typid != InvalidOid)
				attr->atttypid = typid;
		}
	}

	ubs->ps_ResultTupleSlot = vector_init_extra_slot(estate, 
		CreateTupleDescCopy(outerPlanState(ubs)->ps_ResultTupleSlot->tts_tupleDescriptor));
}


static TupleTableSlot*
FetchRowFromBatch(UnbatchState *ubs)
{
	VectorTupleSlot *vslot = (VectorTupleSlot *)ubs->ps_ResultTupleSlot;
	TupleTableSlot *slot = ubs->css.ss.ps.ps_ResultTupleSlot;
	int	idx = ubs->resultVectorIndex;
	
	int	natts;
	int	i;

	while(idx < vslot->dim && vslot->skip[idx])
	{
		for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		{
			Form_pg_attribute attributeForm = TupleDescAttr(slot->tts_tupleDescriptor, i);
			int columnTypeLength = attributeForm->attlen;
			ubs->columnRowOffset[i] += columnTypeLength;
		}
		idx++;
	}

	if (idx == vslot->dim)
		return NULL;

	ExecClearTuple(slot);

	natts = slot->tts_tupleDescriptor->natts;

	for (i = 0; i < natts; i++)
	{
		Vtype* column = (Vtype*) vslot->tts.tts_values[i];

		Form_pg_attribute attributeForm = TupleDescAttr(slot->tts_tupleDescriptor, i);

		int8 *rawColumRawData = (int8*)column->values + ubs->columnRowOffset[i];

		bool columnTypeByValue = attributeForm->attbyval;
		int columnTypeLength = attributeForm->attlen;

		slot->tts_values[i] = fetch_att(rawColumRawData, columnTypeByValue, columnTypeLength);
		ubs->columnRowOffset[i] += columnTypeLength;
		slot->tts_isnull[i] = false;
	}

	ubs->resultVectorIndex = ++idx;

	return ExecStoreVirtualTuple(slot);
}


static TupleTableSlot *
ExecUnbatch(CustomScanState *node)
{
	UnbatchState *ubs = (UnbatchState*) node;
	TupleTableSlot *slot;

	while(true)
	{
		slot = FetchRowFromBatch(ubs);
		if(slot)
			break;

		if (!ReadNextVectorSlot(ubs))
			return NULL;
	}

	return slot;
}


static bool
ReadNextVectorSlot(UnbatchState *ubs)
{
	TupleTableSlot *slot = ExecProcNode(ubs->css.ss.ps.lefttree);

	if(TupIsNull(slot))
		return false;

	slot_getallattrs(slot);

	if (ubs->columnRowOffset) 
		pfree(ubs->columnRowOffset);

	ubs->ps_ResultTupleSlot = slot;
	ubs->columnRowOffset = palloc0(sizeof(int) * slot->tts_tupleDescriptor->natts);
	ubs->resultVectorIndex = 0;
	
	return true;
}


static void
EndUnbatch(CustomScanState *node)
{
	UnbatchState *ubs = (UnbatchState*) node;
	PlanState *outerPlan = outerPlanState(ubs);
	ExecEndNode(outerPlan);

	if (ubs->columnRowOffset) 
		pfree(ubs->columnRowOffset);
}


static Node *
CreateUnbatchState(CustomScan *custom_plan)
{
	UnbatchState *ubs = (UnbatchState *) newNode(
		sizeof(UnbatchState), T_CustomScanState);

	CustomScanState *cscanstate = &ubs->css;
	cscanstate->methods = &UnbatchNodeExecMethods;

	return (Node *) cscanstate;
}


Plan *
columnar_add_unbatch_node(Plan *node)
{
	CustomScan *convert = makeNode(CustomScan);
	
	convert->scan.plan.targetlist = CustomBuildTlist(node->targetlist);
	convert->custom_scan_tlist = node->targetlist;
	convert->methods = &UnbatchNodeMethods;
	convert->scan.plan.lefttree = node;
	convert->scan.plan.righttree = NULL;

	return &convert->scan.plan;
}


void
columnar_unbatch_node_register(void)
{
	RegisterCustomScanMethods(&UnbatchNodeMethods);
}