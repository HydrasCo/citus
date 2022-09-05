/*-------------------------------------------------------------------------
 *
 * columnar_vectorscan.c
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/skey.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_statistic.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"
#include "executor/executor.h"

#include "columnar/columnar.h"
#include "columnar/columnar_tableam.h"

#include "columnar/columnar_utils.h"

#include "columnar/vectorization/columnar_vectorscan.h"
#include "columnar/vectorization/utils.h"
#include "columnar/vectorization/columnar_vectortupleslot.h"
#include "columnar/vectorization/vtype/vtype.h"

typedef TupleTableSlot *(*VExecScanAccessMtd) (VectorScanState *node);
typedef bool (*VExecScanRecheckMtd) (VectorScanState *node, TupleTableSlot *slot);

static Node * CreateVectorScanState(CustomScan *cscan);
static void BeginVectorScan(CustomScanState *node, EState *estate,
										 int eflags);
static void 
InitVectorScanResultSlot(VectorScanState *node, EState *estate, int eflags);
TupleTableSlot * VExecScan(VectorScanState *vss,
						   VExecScanAccessMtd accessMtd,
		  				   VExecScanRecheckMtd recheckMtd);
bool VExecScanQual(ExprState *qual, ExprContext *econtext, bool resultForNull);
static TupleTableSlot * VExecSeqScan(VectorScanState *node);
static TupleTableSlot * ExecVectorScan(CustomScanState *node);
static void EndVectorScan(CustomScanState *node);
static void ReScanVectorScan(CustomScanState *node);
static void ExplainVectorScan(CustomScanState *node, List *ancestors, 
							  ExplainState *es);

const struct CustomScanMethods VectorSeqScan_ScanMethods = {
	.CustomName = "VectorScan",
	.CreateCustomScanState = CreateVectorScanState,
};

const struct CustomExecMethods VectorExecuteMethods = {
	.CustomName = "VectorScan",

	.BeginCustomScan = BeginVectorScan,
	.ExecCustomScan = ExecVectorScan,
	.EndCustomScan = EndVectorScan,
	.ReScanCustomScan = ReScanVectorScan,

	.ExplainCustomScan = ExplainVectorScan,
};


static Node *
CreateVectorScanState(CustomScan *custom_plan)
{
	VectorScanState *vss = (VectorScanState *) newNode(
		sizeof(VectorScanState), T_CustomScanState);

	CustomScanState *cscanstate = &vss->css;
	cscanstate->methods = &VectorExecuteMethods;

	return (Node *) cscanstate;
}


static void
BeginVectorScan(CustomScanState *css, EState *estate, int eflags)
{
	VectorScanState *vss = (VectorScanState *)css;
	CustomScan *cscan = (CustomScan *)css->ss.ps.plan;
	ExprContext *stdecontext = css->ss.ps.ps_ExprContext;

	/* Clean out the 'normal' tuple table slots */
	ExecClearTuple(css->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(css->ss.ss_ScanTupleSlot);

	// Init Scan Slot
	InitVectorScanResultSlot(vss, estate, eflags);
	// Init Result Slot
	vector_init_result_slot(estate, &css->ss.ps);

	ExecAssignExprContext(estate, &css->ss.ps);
	vss->css_RuntimeContext = css->ss.ps.ps_ExprContext;
	css->ss.ps.ps_ExprContext = stdecontext;

	// ProjectionInfo
	ExecAssignScanProjectionInfo(&css->ss);

	ResetExprContext(vss->css_RuntimeContext);
	List *plainClauses = linitial(cscan->custom_exprs);
	vss->qual = (List *) EvalParamsMutator(
		(Node *) plainClauses, vss->css_RuntimeContext);

}


static void
ReScanVectorScan(CustomScanState *node)
{
	elog(ERROR, "Vectorized rescan not implemented yet.");
}


static TupleTableSlot *
ExecVectorScan(CustomScanState *node)
{
	return VExecSeqScan((VectorScanState *)node);
}

static void
EndVectorScan(CustomScanState *node)
{
	VectorScanState *vss = (VectorScanState *)node;

	TableScanDesc scanDesc;

	scanDesc = vss->css.ss.ss_currentScanDesc;

	ExecFreeExprContext(&vss->css.ss.ps);

	// HYDRA: maybe relase memory of columns at this point ?
	vector_clear_slot(vss->css.ss.ps.ps_ResultTupleSlot);
	vector_clear_slot(vss->css.ss.ss_ScanTupleSlot);

	if (scanDesc != NULL)
		table_endscan(scanDesc);

}

static void
ExplainVectorScan(CustomScanState *node, List *ancestors, ExplainState *es)
{
	VectorScanState *vectorScanState = (VectorScanState *) node;

	List *context = set_deparse_context_planstate(
		es->deparse_cxt, (Node *) &node->ss.ps, ancestors);

	List *projectedColumns = ColumnarVarNeeded(&vectorScanState->css);
	const char *projectedColumnsStr = ColumnarProjectedColumnsStr(
		context, projectedColumns);
	ExplainPropertyText("Columnar Projected Columns",
						projectedColumnsStr, es);

	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	List *chunkGroupFilter = lsecond(cscan->custom_exprs);
	if (chunkGroupFilter != NULL)
	{
		const char *pushdownClausesStr = ColumnarPushdownClausesStr(
			context, chunkGroupFilter);
		ExplainPropertyText("Columnar Chunk Group Filters",
							pushdownClausesStr, es);

		ColumnarScanDesc columnarScanDesc =
			(ColumnarScanDesc) node->ss.ss_currentScanDesc;
		if (columnarScanDesc != NULL)
		{
			ExplainPropertyInteger(
				"Columnar Chunk Groups Removed by Filter",
				NULL, ColumnarScanChunkGroupsFiltered(columnarScanDesc), es);
		}
	}
}

static TupleTableSlot *
VSeqNext(VectorScanState *vss)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	scandesc = vss->css.ss.ss_currentScanDesc;
	estate = vss->css.ss.ps.state;
	direction = estate->es_direction;
	slot = vss->css.ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		Bitmapset *attr_needed = ColumnarAttrNeeded(&vss->css.ss);

		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = columnar_beginscan_extended(vss->css.ss.ss_currentRelation,
											   estate->es_snapshot,
											   0, NULL, NULL, 0, attr_needed,
											   vss->qual);
		bms_free(attr_needed);

		vss->css.ss.ss_currentScanDesc = scandesc;
	}

	vector_clear_slot(slot);

	if(columnar_getnextvector(scandesc, direction, slot, VECTOR_BATCH_SIZE))
	{
		return slot;
	}

	return NULL;
}

static bool
VSeqRecheck(VectorScanState *node, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
VExecSeqScan(VectorScanState *node)
{
	return VExecScan(node,
					 (VExecScanAccessMtd) VSeqNext,
					 (VExecScanRecheckMtd) VSeqRecheck);
}


static inline TupleTableSlot *
ExecScanFetch(VectorScanState *vss,
			  VExecScanAccessMtd accessMtd,
			  VExecScanRecheckMtd recheckMtd)
{
	EState			*estate;
	
	estate = vss->css.ss.ps.state;

	// Hydra: TO CHECK THIS CONDITION
	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;
		Index		scanrelid = ((Scan *) vss->css.ss.ps.plan)->scanrelid;

		if (scanrelid == 0)
		{
			TupleTableSlot *slot = vss->css.ss.ss_ScanTupleSlot;

			if (!(*recheckMtd) (vss, slot))
				ExecClearTuple(slot);

			return slot;
		}
		else if (epqstate->relsubs_done[scanrelid - 1])
		{
			TupleTableSlot *slot = vss->css.ss.ss_ScanTupleSlot;

			/* Return empty slot, as we already returned a tuple */
			return ExecClearTuple(slot);
		}
	}

	return (*accessMtd) (vss);
}

TupleTableSlot *
VExecScan(VectorScanState *vss,
		  VExecScanAccessMtd accessMtd,
		  VExecScanRecheckMtd recheckMtd)
{
	ExprContext		*econtext;
	ExprState  		*qual;
	ProjectionInfo	*projInfo;
	TupleTableSlot	*resultProjSlot;
	ScanState 		*node = (ScanState *)&vss->css.ss;

	/*
	 * Fetch data from node
	 */
	qual = node->ps.qual;
	projInfo = node->ps.ps_ProjInfo;
	econtext = node->ps.ps_ExprContext;

	if (!qual && !projInfo)
	{
		ResetExprContext(econtext);
		return ExecScanFetch(vss, accessMtd, recheckMtd);
	}

	ResetExprContext(econtext);

	for (;;)
	{
		TupleTableSlot *slot;

		slot = ExecScanFetch(vss, accessMtd, recheckMtd);

		if (TupIsNull(slot))
		{
			if (projInfo)
				return vector_clear_slot(projInfo->pi_state.resultslot);
			else
				return slot;
		}

		econtext->ecxt_scantuple = slot;

		if (qual == NULL || VExecScanQual(qual, econtext, false))
		{
			if (projInfo)
			{
				resultProjSlot = ExecProject(projInfo);
				memcpy(((VectorTupleSlot*)resultProjSlot)->skip, ((VectorTupleSlot*)slot)->skip, sizeof(bool) *  ((VectorTupleSlot*)slot)->dim);
				((VectorTupleSlot*)resultProjSlot)->dim = ((VectorTupleSlot*)slot)->dim;
				return resultProjSlot;
			}
			else
			{
				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);
	}
}

bool
VExecScanQual(ExprState *qual, ExprContext *econtext, bool resultForNull)
{
	MemoryContext	oldContext;
	TupleTableSlot	*slot;
	VectorTupleSlot	*vslot;

	/* short-circuit (here and in ExecInitQual) for empty restriction list */
	if (qual == NULL)
		return true;

	/*
	 * Run in short-lived per-tuple context while computing expressions.
	 */
	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/*
	 * Evaluate the qual conditions one at a time.  If we find a FALSE result,
	 * we can stop evaluating and return FALSE --- the AND result must be
	 * FALSE.  Also, if we find a NULL result when resultForNull is FALSE, we
	 * can stop and return FALSE --- the AND result must be FALSE or NULL in
	 * that case, and the caller doesn't care which.
	 *
	 * If we get to the end of the list, we can return TRUE.  This will happen
	 * when the AND result is indeed TRUE, or when the AND result is NULL (one
	 * or more NULL subresult, with all the rest TRUE) and the caller has
	 * specified resultForNull = TRUE.
	 */

	slot = econtext->ecxt_scantuple;
	vslot = (VectorTupleSlot *)slot;


	Datum		expr_value;
	bool		isNull;
	vbool		*expr_val_bools;
	int			row;

	/* take a batch as input to evaluate quals */
	expr_value = ExecEvalExpr(qual, econtext, &isNull);
	
	expr_val_bools = (vbool *)DatumGetPointer(expr_value);
	
	/* using skip array to indicated row which didn't pass the qual */
	for(row = 0; row < VECTOR_BATCH_SIZE; row++)
	{
		bool *b = (bool *)expr_val_bools->values + row;
		if((!expr_val_bools->isnull[row] || !resultForNull) && 
			!*b &&
			!vslot->skip[row])
		{
			vslot->skip[row] = true;
		}
	}

	MemoryContextSwitchTo(oldContext);

	/* return true if any tuple in batch pass the qual. */
	for(row = 0; row < VECTOR_BATCH_SIZE; row++)
		if (!vslot->skip[row])
			return true;

	return false;
}


static void
InitVectorScanResultSlot(VectorScanState *node, EState *estate, int eflags)
{
	TupleDesc		vectorTupleDesc;
	int 			i;

	/*
	 * Since we will change the attr type of tupledesc to vector
	 * type. We need to copy it to avoid dirty the relcache.
	 */
	vectorTupleDesc = CreateTupleDescCopy(RelationGetDescr(node->css.ss.ss_currentRelation));

	/* Change the attr type of tupledesc to vector type */
	for (i = 0; i < vectorTupleDesc->natts; i++)
	{
		Form_pg_attribute	attr = TupleDescAttr(vectorTupleDesc, i);
		Oid					vTypId = get_vector_type(attr->atttypid);
		if (vTypId != InvalidOid)
			attr->atttypid = vTypId;
		else
			elog(WARNING, "cannot find vectorized type for type %d", attr->atttypid);
	}

	vector_init_scan_slot(estate, &node->css.ss, vectorTupleDesc);
}


const CustomScanMethods *
columnar_vectorscan_methods(void)
{
	return &VectorSeqScan_ScanMethods;
}


void
columnar_vectorscan_register()
{
	RegisterCustomScanMethods(&VectorSeqScan_ScanMethods);
}