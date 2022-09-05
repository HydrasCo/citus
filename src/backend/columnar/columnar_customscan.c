/*-------------------------------------------------------------------------
 *
 * columnar_customscan.c
 *
 * This file contains the implementation of a postgres custom scan that
 * we use to push down the projections into the table access methods.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "columnar/columnar.h"
#include "columnar/columnar_customscan.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_utils.h"

/*
 * ColumnarScanState represents the state for a columnar scan. It's a
 * CustomScanState with additional fields specific to columnar scans.
 */
typedef struct ColumnarScanState
{
	CustomScanState custom_scanstate; /* must be first field */

	ExprContext *css_RuntimeContext;
	List *qual;
} ColumnarScanState;

/* hooks and callbacks */
static Node * ColumnarScan_CreateCustomScanState(CustomScan *cscan);
static void ColumnarScan_BeginCustomScan(CustomScanState *node, EState *estate,
										 int eflags);
static TupleTableSlot * ColumnarScan_ExecCustomScan(CustomScanState *node);
static void ColumnarScan_EndCustomScan(CustomScanState *node);
static void ColumnarScan_ReScanCustomScan(CustomScanState *node);
static void ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
										   ExplainState *es);

const struct CustomScanMethods ColumnarCustomScanMethods = {
	.CustomName = "ColumnarScan",
	.CreateCustomScanState = ColumnarScan_CreateCustomScanState,
};

const struct CustomExecMethods ColumnarScanExecuteMethods = {
	.CustomName = "ColumnarScan",

	.BeginCustomScan = ColumnarScan_BeginCustomScan,
	.ExecCustomScan = ColumnarScan_ExecCustomScan,
	.EndCustomScan = ColumnarScan_EndCustomScan,
	.ReScanCustomScan = ColumnarScan_ReScanCustomScan,

	.ExplainCustomScan = ColumnarScan_ExplainCustomScan,
};


static Node *
ColumnarScan_CreateCustomScanState(CustomScan *cscan)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) newNode(
		sizeof(ColumnarScanState), T_CustomScanState);

	CustomScanState *cscanstate = &columnarScanState->custom_scanstate;
	cscanstate->methods = &ColumnarScanExecuteMethods;

	return (Node *) cscanstate;
}


static void
ColumnarScan_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags)
{
	CustomScan *cscan = (CustomScan *) cscanstate->ss.ps.plan;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) cscanstate;
	ExprContext *stdecontext = cscanstate->ss.ps.ps_ExprContext;

	/*
	 * Make a new ExprContext just like the existing one, except that we don't
	 * reset it every tuple.
	 */
	ExecAssignExprContext(estate, &cscanstate->ss.ps);
	columnarScanState->css_RuntimeContext = cscanstate->ss.ps.ps_ExprContext;
	cscanstate->ss.ps.ps_ExprContext = stdecontext;

	ResetExprContext(columnarScanState->css_RuntimeContext);
	List *plainClauses = linitial(cscan->custom_exprs);
	columnarScanState->qual = (List *) EvalParamsMutator(
		(Node *) plainClauses, columnarScanState->css_RuntimeContext);

	/* scan slot is already initialized */
}


static TupleTableSlot *
ColumnarScanNext(ColumnarScanState *columnarScanState)
{
	CustomScanState *node = (CustomScanState *) columnarScanState;

	/*
	 * get information from the estate and scan state
	 */
	TableScanDesc scandesc = node->ss.ss_currentScanDesc;
	EState *estate = node->ss.ps.state;
	ScanDirection direction = estate->es_direction;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/* the columnar access method does not use the flags, they are specific to heap */
		uint32 flags = 0;
		Bitmapset *attr_needed = ColumnarAttrNeeded(&node->ss);

		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = columnar_beginscan_extended(node->ss.ss_currentRelation,
											   estate->es_snapshot,
											   0, NULL, NULL, flags, attr_needed,
											   columnarScanState->qual);
		bms_free(attr_needed);

		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
	{
		return slot;
	}
	return NULL;
}


/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
ColumnarScanRecheck(ColumnarScanState *node, TupleTableSlot *slot)
{
	return true;
}


static TupleTableSlot *
ColumnarScan_ExecCustomScan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) ColumnarScanNext,
					(ExecScanRecheckMtd) ColumnarScanRecheck);
}


static void
ColumnarScan_EndCustomScan(CustomScanState *node)
{
	/*
	 * get information from node
	 */
	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
	{
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	}
	
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
	{
		table_endscan(scanDesc);
	}
}


static void
ColumnarScan_ReScanCustomScan(CustomScanState *node)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	ResetExprContext(columnarScanState->css_RuntimeContext);
	List *allClauses = lsecond(cscan->custom_exprs);
	columnarScanState->qual = (List *) EvalParamsMutator(
		(Node *) allClauses, columnarScanState->css_RuntimeContext);

	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;

	if (scanDesc != NULL)
	{
		/* XXX: hack to pass quals as scan keys */
		ScanKey scanKeys = (ScanKey) columnarScanState->qual;
		table_rescan(node->ss.ss_currentScanDesc,
					 scanKeys);
	}
}


static void
ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
							   ExplainState *es)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	List *context = set_deparse_context_planstate(
		es->deparse_cxt, (Node *) &node->ss.ps, ancestors);

	List *projectedColumns = ColumnarVarNeeded(&columnarScanState->custom_scanstate);
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


const CustomScanMethods *
columnar_customscan_methods(void)
{
	return &ColumnarCustomScanMethods;
}


void
columnar_customscan_register(void)
{
	RegisterCustomScanMethods(&ColumnarCustomScanMethods);
}