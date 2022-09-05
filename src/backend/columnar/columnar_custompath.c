/*-------------------------------------------------------------------------
 *
 * columnar_custompath.c
 *
 * This file contains the implementation of a postgres custom scan that
 * we use to push down the projections into the table access methods.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "optimizer/restrictinfo.h"

#include "columnar/columnar.h"
#include "columnar/columnar_custompath.h"
#include "columnar/columnar_customscan.h"
#include "columnar/columnar_utils.h"

#include "columnar/vectorization/columnar_vectorscan.h"

static Plan * ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
											  RelOptInfo *rel,
											  struct CustomPath *best_path,
											  List *tlist,
											  List *clauses,
											  List *custom_plans);

static List * ColumnarScanPath_ReparameterizeCustomPathByChild(PlannerInfo *root,
															   List *custom_private,
															   RelOptInfo *child_rel);

 const struct CustomPathMethods ColumnarPathMethods = {
	.CustomName = "ColumnarScan",
	.PlanCustomPath = ColumnarScanPath_PlanCustomPath,
	.ReparameterizeCustomPathByChild = ColumnarScanPath_ReparameterizeCustomPathByChild,
};


static Plan *
ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
								RelOptInfo *rel,
								struct CustomPath *best_path,
								List *tlist,
								List *clauses,
								List *custom_plans)
{
	/*
	 * Must return a CustomScan, not a larger structure containing a
	 * CustomScan as the first field. Otherwise, copyObject() will fail to
	 * copy the additional fields.
	 */
	CustomScan *cscan = makeNode(CustomScan);

	if (columnar_enable_vectorization)
		cscan->methods = columnar_vectorscan_methods();
	else
		cscan->methods = columnar_customscan_methods();

	/* XXX: also need to store projected column list for EXPLAIN */

	if (columnar_enable_qual_pushdown)
	{
		/*
		 * Lists of pushed-down clauses. The Vars in custom_exprs referencing
		 * other relations will be changed into exec Params by
		 * create_customscan_plan().
		 *
		 * Like CustomPath->custom_private, keep a list of plain clauses
		 * separate from the list of all clauses by making them sublists of a
		 * 2-element list.
		 *
		 * XXX: custom_exprs are the quals that will be pushed into the
		 * columnar reader code; some of these may not be usable. We should
		 * fix this by processing the quals more completely and using
		 * ScanKeys.
		 */
		List *plainClauses = extract_actual_clauses(
			linitial(best_path->custom_private), false /* no pseudoconstants */);
		List *allClauses = extract_actual_clauses(
			lsecond(best_path->custom_private), false /* no pseudoconstants */);
		cscan->custom_exprs = copyObject(list_make2(plainClauses, allClauses));
	}
	else
	{
		cscan->custom_exprs = list_make2(NIL, NIL);
	}

	cscan->scan.plan.qual = extract_actual_clauses(
		clauses, false /* no pseudoconstants */);
	cscan->scan.plan.targetlist = list_copy(tlist);
	cscan->scan.scanrelid = best_path->path.parent->relid;

	return (Plan *) cscan;
}


/*
 * ColumnarScanPath_ReparameterizeCustomPathByChild is a method called when a
 * path is reparameterized directly to a child relation, rather than the
 * top-level parent.
 *
 * For instance, let there be a join of two partitioned columnar relations PX
 * and PY. A path for a ColumnarScan of PY3 might be parameterized by PX so
 * that the join qual "PY3.a = PX.a" (referencing the parent PX) can be pushed
 * down. But if the planner decides on a partition-wise join, then the path
 * will be reparameterized on the child table PX3 directly.
 *
 * When that happens, we need to update all Vars in the pushed-down quals to
 * reference PX3, not PX, to match the new parameterization. This method
 * notifies us that it needs to be done, and allows us to update the
 * information in custom_private.
 */
static List *
ColumnarScanPath_ReparameterizeCustomPathByChild(PlannerInfo *root,
												 List *custom_private,
												 RelOptInfo *child_rel)
{
	return (List *) ReparameterizeMutator((Node *) custom_private, child_rel);
}


const CustomPathMethods *
columnar_custompath_methods()
{
	return &ColumnarPathMethods;
}