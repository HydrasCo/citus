/*-------------------------------------------------------------------------
 *
 * columnar_hooks.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/amapi.h"
#include "catalog/pg_am.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "tcop/utility.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/spccache.h"

#include "columnar/utils/listutils.h"

#include "columnar/columnar.h"
#include "columnar/columnar_custompath.h"
#include "columnar/columnar_customscan.h"
#include "columnar/columnar_hooks.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_utils.h"
#include "columnar/columnar_version_compat.h"

#include "columnar/vectorization/columnar_unbatch_node.h"
#include "columnar/vectorization/columnar_agg_node.h"
#include "columnar/vectorization/columnar_vectorscan.h"
#include "columnar/vectorization/utils.h"

typedef bool (*PathPredicate)(Path *path);

/* saved hook value in case of unload */
static set_rel_pathlist_hook_type PreviousSetRelPathlistHook = NULL;
static get_relation_info_hook_type PreviousGetRelationInfoHook = NULL;
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static planner_hook_type PreviousPlannerHook = NULL;

/* columnar hooks */
static void ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
									   RangeTblEntry *rte);
static void ColumnarGetRelationInfoHook(PlannerInfo *root, Oid relationObjectId,
										bool inhparent, RelOptInfo *rel);
static PlannedStmt * ColumnarVectorPlanerHook(Query *parse,  const char *query_string,
											  int cursorOptions, ParamListInfo boundParams);

static void ColumnarProcessUtility(PlannedStmt *pstmt,
								   const char *queryString,
#if PG_VERSION_NUM >= PG_VERSION_14
								   bool readOnlyTree,
#endif
								   ProcessUtilityContext context,
								   ParamListInfo params,
								   struct QueryEnvironment *queryEnv,
								   DestReceiver *dest,
								   QueryCompletionCompat *completionTag);


static void MutatePlanFields(Plan *newplan, Plan *oldplan, Node *(*mutator) ());
static Node * PlanTreeMutator(Node *node, Node *(*mutator) ());
static Plan * ReplacePlanNodeWalker(Node *node);


/* functions to cost paths in-place */

static void CostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId);
static void CostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
								  IndexPath *indexPath);
static void CostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path);
static void CostColumnarScan(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
							 CustomPath *cpath, int numberOfColumnsRead,
							 int nClauses);


/* functions to add new paths */
static void AddColumnarScanPaths(PlannerInfo *root, RelOptInfo *rel,
								 RangeTblEntry *rte);
static void AddColumnarScanPath(PlannerInfo *root, RelOptInfo *rel,
								RangeTblEntry *rte, Relids required_relids);

/* helper functions to be used when costing paths or altering them */
static void RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate);
static bool IsNotIndexPath(Path *path);
static Cost ColumnarIndexScanAdditionalCost(PlannerInfo *root, RelOptInfo *rel,
											Oid relationId, IndexPath *indexPath);
static int RelationIdGetNumberOfAttributes(Oid relationId);
static Cost ColumnarPerStripeScanCost(RelOptInfo *rel, Oid relationId,
									  int numberOfColumnsRead);
static uint64 ColumnarTableStripeCount(Oid relationId);
static Path * CreateColumnarSeqScanPath(PlannerInfo *root, RelOptInfo *rel,
										Oid relationId);
static void AddColumnarScanPathsRec(PlannerInfo *root, RelOptInfo *rel,
									RangeTblEntry *rte, Relids paramRelids,
									Relids candidateRelids,
									int depthLimit);

/* planner_hook */

static PlannedStmt *
ColumnarVectorPlanerHook(Query *parse,
						 const char *query_string,
				 		 int cursorOptions,
				 		 ParamListInfo boundParams)
{
	PlannedStmt	*stmt;
	Plan *savedPlanTree;
	List *savedSubplan;
	MemoryContext saved_context;

	if (PreviousPlannerHook)
		stmt = PreviousPlannerHook(parse, query_string, cursorOptions, boundParams);
	else
		stmt = standard_planner(parse, query_string, cursorOptions, boundParams);

	if (!columnar_enable_vectorization
		|| stmt->commandType != CMD_SELECT /* only SELECTS are supported  */
		|| list_length(stmt->rtable) != 1) /* JOINs are not yet supported */
		return stmt;

	/* modify plan by using vectorized nodes */
	savedPlanTree = stmt->planTree;
	savedSubplan = stmt->subplans;

	saved_context = CurrentMemoryContext;

	PG_TRY();
	{
		List		*subplans = NULL;
		ListCell	*cell;

		stmt->planTree = ReplacePlanNodeWalker((Node *) stmt->planTree);

		foreach(cell, stmt->subplans)
		{
			Plan	*subplan;
			subplan = ReplacePlanNodeWalker((Node *)lfirst(cell));
			subplans = lappend(subplans, subplan);
		}
		stmt->subplans = subplans;

		/*
		 * vectorize executor exchange batch of tuples between plan nodes
		 * add unbatch node at top to convert batch to row and send to client.
		 */
		stmt->planTree = columnar_add_unbatch_node(stmt->planTree);
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContextSwitchTo(saved_context);

		edata = CopyErrorData();
		FlushErrorState();
		if (columnar_enable_vectorization)
			ereport(NOTICE,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("query can't be vectorized"),
					 errdetail("%s", edata->message)));
		stmt->planTree = savedPlanTree;
		stmt->subplans = savedSubplan;
	}
	PG_END_TRY();

	return stmt;
}

/*
 * We check the expressions tree recursively becuase the args can be a sub expression,
 * we must check the return type of sub expression to fit the parent expressions.
 * so the retType in Vectorized is a temporary values, after we check on expression,
 * we set the retType of this expression, and transfer this value to his parent.
 */
static Oid
GetNodeReturnType(Node *node)
{
	switch(nodeTag(node))
	{
		case T_Var:
			return ((Var*)node)->vartype;
		case T_Const:
			return ((Const*)node)->consttype;
		case T_OpExpr:
			return ((OpExpr*)node)->opresulttype;
		case T_BoolExpr:
			return BOOLOID;
		default:
		{
			elog(ERROR, "Node return type %d not supported", nodeTag(node));
		}
	}
}

/*
 * Check all the expressions if they can be vectorized
 * NOTE: if an expressions is vectorized, we return false...,because we should check
 * all the expressions in the Plan node, if we return true, then the walker will be
 * over...
 */
static Node*
VectorizeMutator(Node *node)
{

	if(NULL == node)
		return NULL;

	//check the type of Var if it can be vectorized
	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var *newnode;
				Oid vtype;
				newnode = (Var*)PlanTreeMutator(node, VectorizeMutator);
				vtype = get_vector_type(newnode->vartype);
				if (InvalidOid == vtype)
					elog(ERROR, "Cannot find vtype for type %d", newnode->vartype);
				newnode->vartype = vtype;
				return (Node *)newnode;
			}

		case T_Aggref:
			{
				Aggref *newnode;
				Oid oldfnOid;
				Oid retype;
				HeapTuple proctup;
				Form_pg_proc procform;
				List *funcname = NULL;
				int i;
				Oid	*argtypes;
				char *proname;
				bool retset;
				int nvargs;
				Oid vatype;
				Oid *true_oid_array;
				FuncDetailCode	fdresult;

				newnode = (Aggref *)PlanTreeMutator(node, VectorizeMutator);
				oldfnOid = newnode->aggfnoid;

				proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(oldfnOid));
				if (!HeapTupleIsValid(proctup))
					elog(ERROR, "cache lookup failed for function %u", oldfnOid);
				procform = (Form_pg_proc) GETSTRUCT(proctup);
				proname = NameStr(procform->proname);
				funcname = lappend(funcname, makeString(proname));

				argtypes = palloc(sizeof(Oid) * procform->pronargs);
				for (i = 0; i < procform->pronargs; i++)
				{
					argtypes[i] = get_vector_type(procform->proargtypes.values[i]);
				}
				
				fdresult = func_get_detail(funcname, NIL, NIL,
						procform->pronargs, argtypes, false, false, false,
						&newnode->aggfnoid, &retype, &retset,
						&nvargs, &vatype,
						&true_oid_array, NULL);

				ReleaseSysCache(proctup);

				// TODO check validation of fdresult.
				if (fdresult != FUNCDETAIL_AGGREGATE || !OidIsValid(newnode->aggfnoid))
					elog(ERROR, "aggreate function not defined");
				return (Node *)newnode;
			}

		case T_BoolExpr:
			{
				BoolExpr *bool_expr;
				OpExpr *bin_expr;
				Oid ltype, rtype;
				Form_pg_operator voper;
				HeapTuple tuple;

				/* mutate OpExpr itself in plan_tree_mutator firstly. */
				bool_expr = (BoolExpr *)PlanTreeMutator(node, VectorizeMutator);

				if (list_length(bool_expr->args) != 2)
					elog(ERROR, "Unary operator not supported");

				ltype = GetNodeReturnType(linitial(bool_expr->args));
				rtype = GetNodeReturnType(lsecond(bool_expr->args));

				//get the vectorized operator functions
				tuple = oper(NULL, list_make1(makeString(bool_expr->boolop == AND_EXPR ? "&" : "|")),
 							 ltype, rtype, true, -1);

				if (NULL == tuple)
					elog(ERROR, "Vectorized operator not found");
			

				voper = (Form_pg_operator)GETSTRUCT(tuple);
				bin_expr = (OpExpr*)make_opclause(voper->oid, voper->oprresult, false,
												  linitial(bool_expr->args),
												  lsecond(bool_expr->args),
												  InvalidOid, InvalidOid);
				bin_expr->opfuncid = voper->oprcode;

				ReleaseSysCache(tuple);
				return (Node *)bin_expr;
			}
		case T_OpExpr:
			{
				OpExpr *newnode;
				Oid	ltype, rtype;
				Oid rettype;
				Form_pg_operator voper;
				HeapTuple tuple;

				/* mutate OpExpr itself in plan_tree_mutator firstly. */
				newnode = (OpExpr *)PlanTreeMutator(node, VectorizeMutator);
				rettype = get_vector_type(newnode->opresulttype);

				if (InvalidOid == rettype)
					elog(ERROR, "Cannot find vtype for type %d", newnode->opresulttype);

				if (list_length(newnode->args) != 2)
					elog(ERROR, "Unary operator not supported");

				ltype = GetNodeReturnType(linitial(newnode->args));
				rtype = GetNodeReturnType(lsecond(newnode->args));

				//get the vectorized operator functions
				tuple = oper(NULL, list_make1(makeString(get_opname(newnode->opno))),
						ltype, rtype, true, -1);

				if(NULL == tuple)
					elog(ERROR, "Vectorized operator not found");
			
				voper = (Form_pg_operator)GETSTRUCT(tuple);

				if(voper->oprresult != rettype)
				{
					ReleaseSysCache(tuple);
					elog(ERROR, "Vectorize operator rettype not correct");
				}

				newnode->opresulttype = rettype;
				newnode->opfuncid = voper->oprcode;

				ReleaseSysCache(tuple);
				return (Node *)newnode;
				break;
			}

		default:
			return PlanTreeMutator(node, VectorizeMutator);
	}
}

static Node *
PlanTreeMutator(Node *node, Node *(*mutator) ())
{
	/*
	 * The mutator has already decided not to modify the current node, but we
	 * must call the mutator for any sub-nodes.
	 */
#define FLATCOPY(newnode, node, nodetype)  \
		( (newnode) = makeNode(nodetype), \
		memcpy((newnode), (node), sizeof(nodetype)) )

#define MUTATE(newfield, oldfield, fieldtype)  \
		( (newfield) = (fieldtype) mutator((Node *) (oldfield)) )

#define PLANMUTATE(newplan, oldplan) \
		MutatePlanFields((Plan*)(newplan), (Plan*)(oldplan), mutator)

#define SCANMUTATE(newplan, oldplan) \
		MutatePlanFields((Plan*)(newplan), (Plan*)(oldplan), mutator)

	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		/* We are expecting here VectorScan */
		case T_CustomScan:
			{
				CustomScan *oldCustomScan = (CustomScan *)node;
				CustomScan *newCustomScan;

				FLATCOPY(newCustomScan, oldCustomScan, CustomScan);
				SCANMUTATE(newCustomScan, node);

				return (Node *) newCustomScan;
			}

		case T_Agg:
			{
				Agg *oldAgg = (Agg *) node;
				Agg	*newAgg;
				CustomScan *aggScanNode;

				if (oldAgg->aggstrategy != AGG_PLAIN && oldAgg->aggstrategy != AGG_HASHED)
					elog(ERROR, "Non plain agg is not supported");

				aggScanNode = make_vectoraggscan_customscan();

				FLATCOPY(newAgg, oldAgg, Agg);

				aggScanNode->custom_plans = lappend(aggScanNode->custom_plans, newAgg);
				aggScanNode->scan.plan.targetlist = CustomBuildTlist(newAgg->plan.targetlist);
				aggScanNode->custom_scan_tlist = newAgg->plan.targetlist;

				SCANMUTATE(newAgg, oldAgg);

				return (Node *) aggScanNode;
			}

		case T_Const:
			{
				Const *oldConst = (Const *) node;
				Const *newConst;

				FLATCOPY(newConst, oldConst, Const);
				return (Node *) newConst;
			}

		case T_Var:
			{
				Var	*oldVar = (Var *) node;
				Var	*newVar;

				FLATCOPY(newVar, oldVar, Var);
				return (Node *) newVar;
			}

		case T_OpExpr:
			{
				OpExpr *oldOpExpr = (OpExpr *) node;
				OpExpr *newOpExpr;

				FLATCOPY(newOpExpr, oldOpExpr, OpExpr);
				MUTATE(newOpExpr->args, oldOpExpr->args, List *);

				return (Node *) newOpExpr;
			}

		case T_FuncExpr:
			{
				FuncExpr *oldFuncExpr = (FuncExpr *) node;
				FuncExpr *newFuncExpr;

				FLATCOPY(newFuncExpr, oldFuncExpr, FuncExpr);
				MUTATE(newFuncExpr->args, oldFuncExpr->args, List *);

				return (Node *) newFuncExpr;
			}

		case T_List:
			{
				/*
				 * We assume the mutator isn't interested in the list nodes
				 * per se, so just invoke it on each list element. NOTE: this
				 * would fail badly on a list with integer elements!
				 */
				List *newResultList =  NULL;
				ListCell *lc;

				foreach(lc, (List *) node)
				{
					newResultList = lappend(newResultList,
											mutator((Node *) lfirst(lc)));
				}

				return (Node *) newResultList;
			}

		case T_TargetEntry:
			{
				TargetEntry *oldTargetEntry = (TargetEntry *) node;
				TargetEntry *newTargetEntry;

				FLATCOPY(newTargetEntry, oldTargetEntry, TargetEntry);
				MUTATE(newTargetEntry->expr, oldTargetEntry->expr, Expr *);

				return (Node *) newTargetEntry;
			}

		case T_Aggref:
			{
				Aggref *oldAggRef = (Aggref *) node;
				Aggref *newAggRef;

				FLATCOPY(newAggRef, oldAggRef, Aggref);
				/* assume mutation doesn't change types of arguments */
				newAggRef->aggargtypes = list_copy(oldAggRef->aggargtypes);
				MUTATE(newAggRef->aggdirectargs, oldAggRef->aggdirectargs, List *);
				MUTATE(newAggRef->args, oldAggRef->args, List *);
				MUTATE(newAggRef->aggorder, oldAggRef->aggorder, List *);
				MUTATE(newAggRef->aggdistinct, oldAggRef->aggdistinct, List *);
				MUTATE(newAggRef->aggfilter, oldAggRef->aggfilter, Expr *);
				return (Node *) newAggRef;
			}
	
		case T_SortGroupClause: 
			{
				SortGroupClause *sortGroupClause = (SortGroupClause *)node;
				SortGroupClause *newSortGroupClause;

				FLATCOPY(newSortGroupClause, sortGroupClause, SortGroupClause);

				return (Node *)newSortGroupClause;
			}
		
		case T_Sort:
			{
				Sort *oldSort = (Sort *) node;
				Sort *newSort;

				FLATCOPY(newSort, oldSort, Sort);
				SCANMUTATE(newSort, oldSort);

				return (Node *)newSort;
			}

		case T_WindowAgg:
			{
				WindowAgg *oldWindowAgg = (WindowAgg *) node;
				WindowAgg *newWindowAgg;

				FLATCOPY(newWindowAgg, oldWindowAgg, WindowAgg);
				SCANMUTATE(newWindowAgg, oldWindowAgg);

				return (Node *) newWindowAgg;
			}

		case T_WindowFunc:
			{
				WindowFunc *oldWindowFunc = (WindowFunc *) node;
				WindowFunc *newWindowFunc;

				FLATCOPY(newWindowFunc, oldWindowFunc, WindowFunc);
				MUTATE(newWindowFunc->args, oldWindowFunc->args, List *);
				MUTATE(newWindowFunc->aggfilter, oldWindowFunc->aggfilter, Expr *);

				return (Node *) newWindowFunc;
			}

	  default:
			elog(ERROR, "Node type %d not supported for vectorization", nodeTag(node));
			break;
	}
}

/* Function mutate_plan_fields() is a subroutine for plan_tree_mutator().
 * It "hijacks" the macro MUTATE defined for use in that function, so don't
 * change the argument names "mutator" and "context" use in the macro
 * definition.
 *
 */
static void
MutatePlanFields(Plan *newplan, Plan *oldplan, Node *(*mutator) ())
{
	List* conjuncts = (List*)mutator((Node*)oldplan->qual);
	int n_conjuncts = list_length(conjuncts);
	if (n_conjuncts > 1)
	{
		int i;
		Form_pg_operator	voper;
		Expr* right = (Expr*)list_nth(conjuncts, n_conjuncts-1);
		Oid vbool_type = GetNodeReturnType((Node*)right);
		HeapTuple tuple = oper(NULL, list_make1(makeString("&")),
							   vbool_type, vbool_type, true, -1);
		if (NULL == tuple)
		{
			elog(ERROR, "Vectorized operator not found");
		}
		voper = (Form_pg_operator)GETSTRUCT(tuple);
		for (i = n_conjuncts-2; i >= 0; i--)
		{
			Expr* left =  (Expr*)list_nth(conjuncts, i);
			Assert(GetNodeReturnType((Node*)left) == vbool_type);
			right = (Expr*)make_opclause(voper->oid, voper->oprresult, false,
										 left, right, InvalidOid, InvalidOid);
			((OpExpr*)right)->opfuncid = voper->oprcode;
		}
		conjuncts = list_make1(right);
		ReleaseSysCache(tuple);
	}

	newplan->qual = conjuncts;

	/*
	 * Scalar fields startup_cost total_cost plan_rows plan_width nParamExec
	 * need no mutation.
	 */

	/* Node fields need mutation. */
	MUTATE(newplan->targetlist, oldplan->targetlist, List *);
	MUTATE(newplan->lefttree, oldplan->lefttree, Plan *);
	MUTATE(newplan->righttree, oldplan->righttree, Plan *);
	MUTATE(newplan->initPlan, oldplan->initPlan, List *);

	/* Bitmapsets aren't nodes but need to be copied to palloc'd space. */
	newplan->extParam = bms_copy(oldplan->extParam);
	newplan->allParam = bms_copy(oldplan->allParam);
}


/*
 * Replace the non-vectorirzed type to vectorized type
 */
static Plan* 
ReplacePlanNodeWalker(Node *node)
{
	return (Plan *)PlanTreeMutator(node, VectorizeMutator);
}


/* get_relation_info_hook */


static void
ColumnarGetRelationInfoHook(PlannerInfo *root, Oid relationObjectId,
							bool inhparent, RelOptInfo *rel)
{
	if (PreviousGetRelationInfoHook)
	{
		PreviousGetRelationInfoHook(root, relationObjectId, inhparent, rel);
	}

	if (IsColumnarTableAmTable(relationObjectId))
	{
		/* disable parallel query */
		rel->rel_parallel_workers = 0;

		/* disable index-only scan */
		IndexOptInfo *indexOptInfo = NULL;
		foreach_ptr(indexOptInfo, rel->indexlist)
		{
			memset(indexOptInfo->canreturn, false, indexOptInfo->ncolumns * sizeof(bool));
		}
	}
}


/* ProcessUtility_hook_type */


static void
ColumnarProcessUtility(PlannedStmt *pstmt,
					   const char *queryString,
#if PG_VERSION_NUM >= PG_VERSION_14
					   bool readOnlyTree,
#endif
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   struct QueryEnvironment *queryEnv,
					   DestReceiver *dest,
					   QueryCompletionCompat *completionTag)
{
#if PG_VERSION_NUM >= PG_VERSION_14
	if (readOnlyTree)
	{
		pstmt = copyObject(pstmt);
	}
#endif

	Node *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, IndexStmt))
	{
		IndexStmt *indexStmt = (IndexStmt *) parsetree;

		Relation rel = relation_openrv(indexStmt->relation,
									   indexStmt->concurrent ? ShareUpdateExclusiveLock :
									   ShareLock);

		if (rel->rd_tableam == GetColumnarTableAmRoutine())
		{
			CheckCitusColumnarVersion(ERROR);
			if (!ColumnarSupportsIndexAM(indexStmt->accessMethod))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("unsupported access method for the "
									   "index on columnar table %s",
									   RelationGetRelationName(rel))));
			}
		}

		RelationClose(rel);
	}

	PrevProcessUtilityHook_compat(pstmt, queryString, false, context,
								  params, queryEnv, dest, completionTag);
}


/* set_rel_pathlist_hook */


static void
ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
						   RangeTblEntry *rte)
{
	/* call into previous hook if assigned */
	if (PreviousSetRelPathlistHook)
	{
		PreviousSetRelPathlistHook(root, rel, rti, rte);
	}

	if (!OidIsValid(rte->relid) || rte->rtekind != RTE_RELATION || rte->inh)
	{
		/* some calls to the pathlist hook don't have a valid relation set. Do nothing */
		return;
	}

	/*
	 * Here we want to inspect if this relation pathlist hook is accessing a columnar table.
	 * If that is the case we want to insert an extra path that pushes down the projection
	 * into the scan of the table to minimize the data read.
	 */
	Relation relation = RelationIdGetRelation(rte->relid);
	if (relation->rd_tableam == GetColumnarTableAmRoutine())
	{
		if (rte->tablesample != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("sample scans not supported on columnar tables")));
		}

		if (list_length(rel->partial_pathlist) != 0)
		{
			/*
			 * Parallel scans on columnar tables are already discardad by
			 * ColumnarGetRelationInfoHook but be on the safe side.
			 */
			elog(ERROR, "parallel scans on columnar are not supported");
		}

		/*
		 * There are cases where IndexPath is normally more preferrable over
		 * SeqPath for heapAM but not for columnarAM. In such cases, an
		 * IndexPath could wrongly dominate a SeqPath based on the costs
		 * estimated by postgres earlier. For this reason, here we manually
		 * create a SeqPath, estimate the cost based on columnarAM and append
		 * to pathlist.
		 *
		 * Before doing that, we first re-cost all the existing paths so that
		 * add_path makes correct cost comparisons when appending our SeqPath.
		 */
		CostColumnarPaths(root, rel, rte->relid);

		Path *seqPath = CreateColumnarSeqScanPath(root, rel, rte->relid);
		add_path(rel, seqPath);

		if (columnar_enable_custom_scan)
		{
			ereport(DEBUG1, (errmsg("pathlist hook for columnar table am")));

			/*
			 * When columnar custom scan is enabled (columnar.enable_custom_scan),
			 * we only consider ColumnarScanPath's & IndexPath's. For this reason,
			 * we remove other paths and re-estimate IndexPath costs to make accurate
			 * comparisons between them.
			 *
			 * Even more, we might calculate an equal cost for a
			 * ColumnarCustomScan and a SeqPath if we are reading all columns
			 * of given table since we don't consider chunk group filtering
			 * when costing ColumnarCustomScan.
			 * In that case, if we don't remove SeqPath's, we might wrongly choose
			 * SeqPath thinking that its cost would be equal to ColumnarCustomScan.
			 */
			RemovePathsByPredicate(rel, IsNotIndexPath);
			AddColumnarScanPaths(root, rel, rte);
		}
	}
	RelationClose(relation);
}

/*
 * CostColumnarPaths re-costs paths of given RelOptInfo for
 * columnar table with relationId.
 */
static void
CostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId)
{
	Path *path = NULL;
	foreach_ptr(path, rel->pathlist)
	{
		if (IsA(path, IndexPath))
		{
			/*
			 * Since we don't provide implementations for scan_bitmap_next_block
			 * & scan_bitmap_next_tuple, postgres doesn't generate bitmap index
			 * scan paths for columnar tables already (see related comments in
			 * TableAmRoutine). For this reason, we only consider IndexPath's
			 * here.
			 */
			CostColumnarIndexPath(root, rel, relationId, (IndexPath *) path);
		}
		else if (path->pathtype == T_SeqScan)
		{
			CostColumnarSeqPath(rel, relationId, path);
		}
	}
}

/*
 * CostColumnarIndexPath re-costs given index path for columnar table with
 * relationId.
 */
static void
CostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
					  IndexPath *indexPath)
{
	if (!enable_indexscan)
	{
		/* costs are already set to disable_cost, don't adjust them */
		return;
	}

	ereport(DEBUG4, (errmsg("columnar table index scan costs estimated by "
							"indexAM: startup cost = %.10f, total cost = "
							"%.10f", indexPath->path.startup_cost,
							indexPath->path.total_cost)));

	/*
	 * We estimate the cost for columnar table read during index scan. Also,
	 * instead of overwriting total cost, we "add" ours to the cost estimated
	 * by indexAM since we should consider index traversal related costs too.
	 */
	Cost columnarIndexScanCost = ColumnarIndexScanAdditionalCost(root, rel, relationId,
																 indexPath);
	indexPath->path.total_cost += columnarIndexScanCost;

	ereport(DEBUG4, (errmsg("columnar table index scan costs re-estimated "
							"by columnarAM (including indexAM costs): "
							"startup cost = %.10f, total cost = %.10f",
							indexPath->path.startup_cost,
							indexPath->path.total_cost)));
}

/*
 * CostColumnarSeqPath sets costs given seq path for columnar table with
 * relationId.
 */
static void
CostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path)
{
	if (!enable_seqscan)
	{
		/* costs are already set to disable_cost, don't adjust them */
		return;
	}

	/*
	 * Seq scan doesn't support projection or qual pushdown, so we will read
	 * all the stripes and all the columns.
	 */
	double stripesToRead = ColumnarTableStripeCount(relationId);
	int numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);

	path->startup_cost = 0;
	path->total_cost = stripesToRead *
					   ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);
}

/*
 * CostColumnarScan calculates the cost of scanning the columnar table. The
 * cost is estimated by using all stripe metadata to estimate based on the
 * columns to read how many pages need to be read.
 */
static void
CostColumnarScan(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
				 CustomPath *cpath, int numberOfColumnsRead, int nClauses)
{
	Path *path = &cpath->path;

	List *allClauses = lsecond(cpath->custom_private);
	Selectivity clauseSel = clauselist_selectivity(
		root, allClauses, rel->relid, JOIN_INNER, NULL);

	/*
	 * We already filtered out clauses where the overall selectivity would be
	 * misleading, such as inequalities involving an uncorrelated column. So
	 * we can apply the selectivity directly to the number of stripes.
	 */
	double stripesToRead = clauseSel * ColumnarTableStripeCount(relationId);
	stripesToRead = Max(stripesToRead, 1.0);

	path->rows = rel->rows;
	path->startup_cost = 0;
	path->total_cost = stripesToRead *
					   ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);
}


/*
 * ExprReferencesRelid returns true if any of the Expr's Vars refer to the
 * given relid; false otherwise.
 */
static bool
ExprReferencesRelid(Expr *expr, Index relid)
{
	List *exprVars = pull_var_clause(
		(Node *) expr, PVC_RECURSE_AGGREGATES |
		PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS);
	ListCell *lc;
	foreach(lc, exprVars)
	{
		Var *var = (Var *) lfirst(lc);
		if (var->varno == relid)
		{
			return true;
		}
	}

	return false;
}


/*
 * CheckVarStats() checks whether a qual involving this Var is likely to be
 * useful based on the correlation stats. If so, or if stats are unavailable,
 * return true; otherwise return false and sets absVarCorrelation in case
 * caller wants to use for logging purposes.
 */
static bool
CheckVarStats(PlannerInfo *root, Var *var, Oid sortop, float4 *absVarCorrelation)
{
	/*
	 * Collect isunique, ndistinct, and varCorrelation.
	 */
	VariableStatData varStatData;
	examine_variable(root, (Node *) var, var->varno, &varStatData);
	if (varStatData.rel == NULL ||
		!HeapTupleIsValid(varStatData.statsTuple))
	{
		return true;
	}

	AttStatsSlot sslot;
	if (!get_attstatsslot(&sslot, varStatData.statsTuple,
						  STATISTIC_KIND_CORRELATION, sortop,
						  ATTSTATSSLOT_NUMBERS))
	{
		ReleaseVariableStats(varStatData);
		return true;
	}

	Assert(sslot.nnumbers == 1);

	float4 varCorrelation = sslot.numbers[0];

	ReleaseVariableStats(varStatData);

	/*
	 * If the Var is not highly correlated, then the chunk's min/max bounds
	 * will be nearly useless.
	 */
	if (Abs(varCorrelation) < columnar_qual_pushdown_correlation_threshold)
	{
		if (absVarCorrelation)
		{
			/*
			 * Report absVarCorrelation if caller wants to know why given
			 * var is rejected.
			 */
			*absVarCorrelation = Abs(varCorrelation);
		}
		return false;
	}

	return true;
}


/*
 * ExtractPushdownClause extracts an Expr node from given clause for pushing down
 * into the given rel (including join clauses). This test may not be exact in
 * all cases; it's used to reduce the search space for parameterization.
 *
 * Note that we don't try to handle cases like "Var + ExtParam = 3". That
 * would require going through eval_const_expression after parameter binding,
 * and that doesn't seem worth the effort. Here we just look for "Var op Expr"
 * or "Expr op Var", where Var references rel and Expr references other rels
 * (or no rels at all).
 *
 * Moreover, this function also looks into BoolExpr's to recursively extract
 * pushdownable OpExpr's of them:
 * i)   AND_EXPR:
 *      Take pushdownable args of AND expressions by ignoring the other args.
 * ii)  OR_EXPR:
 *      Ignore the whole OR expression if we cannot exract a pushdownable Expr
 *      from one of its args.
 * iii) NOT_EXPR:
 *      Simply ignore NOT expressions since we don't expect to see them before
 *      an expression that we can pushdown, see the comment in function.
 *
 * The reasoning for those three rules could also be summarized as such;
 * for any expression that we cannot push-down, we must assume that it
 * evaluates to true.
 *
 * For example, given following WHERE clause:
 * (
 *     (a > random() OR a < 30)
 *     AND
 *     a < 200
 * ) OR
 * (
 *     a = 300
 *     OR
 *     a > 400
 * );
 * Even if we can pushdown (a < 30), we cannot pushdown (a > random() OR a < 30)
 * due to (a > random()). However, we can pushdown (a < 200), so we extract
 * (a < 200) from the lhs of the top level OR expression.
 *
 * For the rhs of the top level OR expression, since we can pushdown both (a = 300)
 * and (a > 400), we take this part as is.
 *
 * Finally, since both sides of the top level OR expression yielded pushdownable
 * expressions, we will pushdown the following:
 *  (a < 200) OR ((a = 300) OR (a > 400))
 */
static Expr *
ExtractPushdownClause(PlannerInfo *root, RelOptInfo *rel, Node *node)
{
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, BoolExpr))
	{
		BoolExpr *boolExpr = castNode(BoolExpr, node);
		if (boolExpr->boolop == NOT_EXPR)
		{
			/*
			 * Standard planner should have already applied de-morgan rule to
			 * simple NOT expressions. If we encounter with such an expression
			 * here, then it can't be a pushdownable one, such as:
			 *   WHERE id NOT IN (SELECT id FROM something).
			 */
			ereport(columnar_planner_debug_level,
					(errmsg("columnar planner: cannot push down clause: "
							"must not contain a subplan")));
			return NULL;
		}

		List *pushdownableArgs = NIL;

		Node *boolExprArg = NULL;
		foreach_ptr(boolExprArg, boolExpr->args)
		{
			Expr *pushdownableArg = ExtractPushdownClause(root, rel,
														  (Node *) boolExprArg);
			if (pushdownableArg)
			{
				pushdownableArgs = lappend(pushdownableArgs, pushdownableArg);
			}
			else if (boolExpr->boolop == OR_EXPR)
			{
				ereport(columnar_planner_debug_level,
						(errmsg("columnar planner: cannot push down clause: "
								"all arguments of an OR expression must be "
								"pushdownable but one of them was not, due "
								"to the reason given above")));
				return NULL;
			}

			/* simply skip AND args that we cannot pushdown */
		}

		int npushdownableArgs = list_length(pushdownableArgs);
		if (npushdownableArgs == 0)
		{
			ereport(columnar_planner_debug_level,
					(errmsg("columnar planner: cannot push down clause: "
							"none of the arguments were pushdownable, "
							"due to the reason(s) given above ")));
			return NULL;
		}
		else if (npushdownableArgs == 1)
		{
			return (Expr *) linitial(pushdownableArgs);
		}

		if (boolExpr->boolop == AND_EXPR)
		{
			return make_andclause(pushdownableArgs);
		}
		else if (boolExpr->boolop == OR_EXPR)
		{
			return make_orclause(pushdownableArgs);
		}
		else
		{
			/* already discarded NOT expr, so should not be reachable */
			return NULL;
		}
	}

	if (!IsA(node, OpExpr) || list_length(((OpExpr *) node)->args) != 2)
	{
		ereport(columnar_planner_debug_level,
				(errmsg("columnar planner: cannot push down clause: "
						"must be binary operator expression")));
		return NULL;
	}

	OpExpr *opExpr = castNode(OpExpr, node);
	Expr *lhs = list_nth(opExpr->args, 0);
	Expr *rhs = list_nth(opExpr->args, 1);

	Var *varSide;
	Expr *exprSide;

	if (IsA(lhs, Var) && ((Var *) lhs)->varno == rel->relid &&
		!ExprReferencesRelid((Expr *) rhs, rel->relid))
	{
		varSide = castNode(Var, lhs);
		exprSide = rhs;
	}
	else if (IsA(rhs, Var) && ((Var *) rhs)->varno == rel->relid &&
			 !ExprReferencesRelid((Expr *) lhs, rel->relid))
	{
		varSide = castNode(Var, rhs);
		exprSide = lhs;
	}
	else
	{
		ereport(columnar_planner_debug_level,
				(errmsg("columnar planner: cannot push down clause: "
						"must match 'Var <op> Expr' or 'Expr <op> Var'"),
				 errhint("Var must only reference this rel, "
						 "and Expr must not reference this rel")));
		return NULL;
	}

	if (varSide->varattno <= 0)
	{
		ereport(columnar_planner_debug_level,
				(errmsg("columnar planner: cannot push down clause: "
						"var is whole-row reference or system column")));
		return NULL;
	}

	if (contain_volatile_functions((Node *) exprSide))
	{
		ereport(columnar_planner_debug_level,
				(errmsg("columnar planner: cannot push down clause: "
						"expr contains volatile functions")));
		return NULL;
	}

	/* only the default opclass is used for qual pushdown. */
	Oid varOpClass = GetDefaultOpClass(varSide->vartype, BTREE_AM_OID);
	Oid varOpFamily;
	Oid varOpcInType;

	if (!OidIsValid(varOpClass) ||
		!get_opclass_opfamily_and_input_type(varOpClass, &varOpFamily,
											 &varOpcInType))
	{
		ereport(columnar_planner_debug_level,
				(errmsg("columnar planner: cannot push down clause: "
						"cannot find default btree opclass and opfamily for type: %s",
						format_type_be(varSide->vartype))));
		return NULL;
	}

	if (!op_in_opfamily(opExpr->opno, varOpFamily))
	{
		ereport(columnar_planner_debug_level,
				(errmsg("columnar planner: cannot push down clause: "
						"operator %d not a member of opfamily %d",
						opExpr->opno, varOpFamily)));
		return NULL;
	}

	Oid sortop = get_opfamily_member(varOpFamily, varOpcInType,
									 varOpcInType, BTLessStrategyNumber);
	Assert(OidIsValid(sortop));

	/*
	 * Check that statistics on the Var support the utility of this
	 * clause.
	 */
	float4 absVarCorrelation = 0;
	if (!CheckVarStats(root, varSide, sortop, &absVarCorrelation))
	{
		ereport(columnar_planner_debug_level,
				(errmsg("columnar planner: cannot push down clause: "
						"absolute correlation (%.3f) of var attribute %d is "
						"smaller than the value configured in "
						"\"columnar.qual_pushdown_correlation_threshold\" "
						"(%.3f)", absVarCorrelation, varSide->varattno,
						columnar_qual_pushdown_correlation_threshold)));
		return NULL;
	}

	return (Expr *) node;
}


/*
 * FilterPushdownClauses filters for clauses that are candidates for pushing
 * down into rel.
 */
static List *
FilterPushdownClauses(PlannerInfo *root, RelOptInfo *rel, List *inputClauses)
{
	List *filteredClauses = NIL;
	ListCell *lc;
	foreach(lc, inputClauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * Ignore clauses that don't refer to this rel, and pseudoconstants.
		 *
		 * XXX: A pseudoconstant may be of use, but it doesn't make sense to
		 * push it down because it doesn't contain any Vars. Look into if
		 * there's something we should do with pseudoconstants here.
		 */
		if (rinfo->pseudoconstant ||
			!bms_is_member(rel->relid, rinfo->required_relids))
		{
			continue;
		}

		Expr *pushdownableExpr = ExtractPushdownClause(root, rel, (Node *) rinfo->clause);
		if (!pushdownableExpr)
		{
			continue;
		}

		rinfo = copyObject(rinfo);
		rinfo->clause = pushdownableExpr;
		filteredClauses = lappend(filteredClauses, rinfo);
	}

	return filteredClauses;
}


/*
 * PushdownJoinClauseMatches is a callback that returns true, indicating that
 * we want all of the clauses from generate_implied_equalities_for_column().
 */
static bool
PushdownJoinClauseMatches(PlannerInfo *root, RelOptInfo *rel,
						  EquivalenceClass *ec, EquivalenceMember *em,
						  void *arg)
{
	return true;
}


/*
 * FindPushdownJoinClauses finds join clauses, including those implied by ECs,
 * that may be pushed down.
 */
static List *
FindPushdownJoinClauses(PlannerInfo *root, RelOptInfo *rel)
{
	List *joinClauses = copyObject(rel->joininfo);

	/*
	 * Here we are generating the clauses just so we can later extract the
	 * interesting relids. This is somewhat wasteful, but it allows us to
	 * filter out joinclauses, reducing the number of relids we need to
	 * consider.
	 *
	 * XXX: also find additional clauses for joininfo that are implied by ECs?
	 */
	List *ecClauses = generate_implied_equalities_for_column(
		root, rel, PushdownJoinClauseMatches, NULL,
		rel->lateral_referencers);
	List *allClauses = list_concat(joinClauses, ecClauses);

	return FilterPushdownClauses(root, rel, allClauses);
}


/*
 * FindCandidateRelids identifies candidate rels for parameterization from the
 * list of join clauses.
 *
 * Some rels cannot be considered for parameterization, such as a partitioned
 * parent of the given rel. Other rels are just not useful because they don't
 * appear in a join clause that could be pushed down.
 */
static Relids
FindCandidateRelids(PlannerInfo *root, RelOptInfo *rel, List *joinClauses)
{
	Relids candidateRelids = NULL;
	ListCell *lc;
	foreach(lc, joinClauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		candidateRelids = bms_add_members(candidateRelids,
										  rinfo->required_relids);
	}

	candidateRelids = bms_del_members(candidateRelids, rel->relids);
	candidateRelids = bms_del_members(candidateRelids, rel->lateral_relids);
	return candidateRelids;
}


/*
 * Combinations() calculates the number of combinations of n things taken k at
 * a time. When the correct result is large, the calculation may produce a
 * non-integer result, or overflow to inf, which caller should handle
 * appropriately.
 *
 * Use the following two formulae from Knuth TAoCP, 1.2.6:
 *    (2) Combinations(n, k) = (n*(n-1)..(n-k+1)) / (k*(k-1)..1)
 *    (5) Combinations(n, k) = Combinations(n, n-k)
 */
static double
Combinations(int n, int k)
{
	double v = 1;

	/*
	 * If k is close to n, then both the numerator and the denominator are
	 * close to n!, and we may overflow even if the input is reasonable
	 * (e.g. Combinations(500, 500)). Use formula (5) to choose the smaller,
	 * but equivalent, k.
	 */
	k = Min(k, n - k);

	/* calculate numerator of formula (2) first */
	for (int i = n; i >= n - k + 1; i--)
	{
		v *= i;
	}

	/*
	 * Divide by each factor in the denominator of formula (2), skipping
	 * division by 1.
	 */
	for (int i = k; i >= 2; i--)
	{
		v /= i;
	}

	return v;
}


/*
 * ChooseDepthLimit() calculates the depth limit for the parameterization
 * search, given the number of candidate relations.
 *
 * The maximum number of paths generated for a given depthLimit is:
 *
 *    Combinations(nCandidates, 0) + Combinations(nCandidates, 1) + ... +
 *    Combinations(nCandidates, depthLimit)
 *
 * There's no closed formula for a partial sum of combinations, so just keep
 * increasing the depth until the number of combinations exceeds the limit.
 */
static int
ChooseDepthLimit(int nCandidates)
{
	if (!columnar_enable_qual_pushdown)
	{
		return 0;
	}

	int depth = 0;
	double numPaths = 1;

	while (depth < nCandidates)
	{
		numPaths += Combinations(nCandidates, depth + 1);

		if (numPaths > (double) columnar_max_custom_scan_paths)
		{
			break;
		}

		depth++;
	}

	return depth;
}


/*
 * AddColumnarScanPaths is the entry point for recursively generating
 * parameterized paths. See AddColumnarScanPathsRec() for discussion.
 */
static void
AddColumnarScanPaths(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	List *joinClauses = FindPushdownJoinClauses(root, rel);
	Relids candidateRelids = FindCandidateRelids(root, rel, joinClauses);

	int depthLimit = ChooseDepthLimit(bms_num_members(candidateRelids));

	/* must always parameterize by lateral refs */
	Relids paramRelids = bms_copy(rel->lateral_relids);

	AddColumnarScanPathsRec(root, rel, rte, paramRelids, candidateRelids,
							depthLimit);
}

/*
 * ContainsExecParams tests whether the node contains any exec params. The
 * signature accepts an extra argument for use with expression_tree_walker.
 */
static bool
ContainsExecParams(Node *node, void *notUsed)
{
	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, Param))
	{
		Param *param = castNode(Param, node);
		if (param->paramkind == PARAM_EXEC)
		{
			return true;
		}
	}
	return expression_tree_walker(node, ContainsExecParams, NULL);
}

/*
 * ParameterizationAsString returns the string representation of the set of
 * rels given in paramRelids.
 *
 * Takes a StringInfo so that it doesn't return palloc'd memory. This makes it
 * easy to call this function as an argument to ereport(), such that it won't
 * be evaluated unless the message is going to be output somewhere.
 */
static char *
ParameterizationAsString(PlannerInfo *root, Relids paramRelids, StringInfo buf)
{
	bool firstTime = true;
	int relid = -1;

	if (bms_num_members(paramRelids) == 0)
	{
		return "unparameterized";
	}

	appendStringInfoString(buf, "parameterized by rels {");
	while ((relid = bms_next_member(paramRelids, relid)) >= 0)
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		const char *relname = quote_identifier(rte->eref->aliasname);

		appendStringInfo(buf, "%s%s", firstTime ? "" : ", ", relname);

		if (relname != rte->eref->aliasname)
		{
			pfree((void *) relname);
		}

		firstTime = false;
	}
	appendStringInfoString(buf, "}");
	return buf->data;
}


/*
 * Create and add a path with the given parameterization paramRelids.
 *
 * XXX: Consider refactoring to be more like postgresGetForeignPaths(). The
 * only differences are param_info and custom_private.
 */
static void
AddColumnarScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
					Relids paramRelids)
{
	/*
	 * Must return a CustomPath, not a larger structure containing a
	 * CustomPath as the first field. Otherwise, nodeToString() will fail to
	 * output the additional fields.
	 */
	CustomPath *cpath = makeNode(CustomPath);

	cpath->methods = columnar_custompath_methods();

	/*
	 * populate generic path information
	 */
	Path *path = &cpath->path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;

	/* columnar scans are not parallel-aware, but they are parallel-safe */
	path->parallel_safe = rel->consider_parallel;

	path->param_info = get_baserel_parampathinfo(root, rel, paramRelids);

	/*
	 * Usable clauses for this parameterization exist in baserestrictinfo and
	 * ppi_clauses.
	 */
	List *allClauses = copyObject(rel->baserestrictinfo);
	if (path->param_info != NULL)
	{
		allClauses = list_concat(allClauses, path->param_info->ppi_clauses);
	}

	allClauses = FilterPushdownClauses(root, rel, allClauses);

	/*
	 * Plain clauses may contain extern params, but not exec params, and can
	 * be evaluated at init time or rescan time. Track them in another list
	 * that is a subset of allClauses.
	 *
	 * Note: although typically baserestrictinfo contains plain clauses,
	 * that's not always true. It can also contain a qual referencing a Var at
	 * a higher query level, which can be turned into an exec param, and
	 * therefore it won't be a plain clause.
	 */
	List *plainClauses = NIL;
	ListCell *lc;
	foreach(lc, allClauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		if (bms_is_subset(rinfo->required_relids, rel->relids) &&
			!ContainsExecParams((Node *) rinfo->clause, NULL))
		{
			plainClauses = lappend(plainClauses, rinfo);
		}
	}

	/*
	 * We can't make our own CustomPath structure, so we need to put
	 * everything in the custom_private list. To keep the two lists separate,
	 * we make them sublists in a 2-element list.
	 */
	if (columnar_enable_qual_pushdown)
	{
		cpath->custom_private = list_make2(copyObject(plainClauses),
										   copyObject(allClauses));
	}
	else
	{
		cpath->custom_private = list_make2(NIL, NIL);
	}

	int numberOfColumnsRead = bms_num_members(rte->selectedCols);
	int numberOfClausesPushed = list_length(allClauses);

	CostColumnarScan(root, rel, rte->relid, cpath, numberOfColumnsRead,
					 numberOfClausesPushed);


	StringInfoData buf;
	initStringInfo(&buf);
	ereport(columnar_planner_debug_level,
			(errmsg("columnar planner: adding CustomScan path for %s",
					rte->eref->aliasname),
			 errdetail("%s; %d clauses pushed down",
					   ParameterizationAsString(root, paramRelids, &buf),
					   numberOfClausesPushed)));

	add_path(rel, path);
}

/*
 * RemovePathsByPredicate removes the paths that removePathPredicate
 * evaluates to true from pathlist of given rel.
 */
static void
RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate)
{
	List *filteredPathList = NIL;

	Path *path = NULL;
	foreach_ptr(path, rel->pathlist)
	{
		if (!removePathPredicate(path))
		{
			filteredPathList = lappend(filteredPathList, path);
		}
	}

	rel->pathlist = filteredPathList;
}

/*
 * IsNotIndexPath returns true if given path is not an IndexPath.
 */
static bool
IsNotIndexPath(Path *path)
{
	return !IsA(path, IndexPath);
}

/*
 * ColumnarIndexScanAdditionalCost returns additional cost estimated for
 * index scan described by IndexPath for columnar table with relationId.
 */
static Cost
ColumnarIndexScanAdditionalCost(PlannerInfo *root, RelOptInfo *rel,
								Oid relationId, IndexPath *indexPath)
{
	int numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);
	Cost perStripeCost = ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);

	/*
	 * We don't need to pass correct loop count to amcostestimate since we
	 * will only use index correlation & index selectivity, and loop count
	 * doesn't have any effect on those two.
	 */
	double fakeLoopCount = 1;
	Cost fakeIndexStartupCost;
	Cost fakeIndexTotalCost;
	double fakeIndexPages;
	Selectivity indexSelectivity;
	double indexCorrelation;
	amcostestimate_function amcostestimate = indexPath->indexinfo->amcostestimate;
	amcostestimate(root, indexPath, fakeLoopCount, &fakeIndexStartupCost,
				   &fakeIndexTotalCost, &indexSelectivity,
				   &indexCorrelation, &fakeIndexPages);

	Relation relation = RelationIdGetRelation(relationId);
	uint64 rowCount = ColumnarTableRowCount(relation);
	RelationClose(relation);
	double estimatedRows = rowCount * indexSelectivity;

	/*
	 * In the worst case (i.e no correlation between the column & the index),
	 * we need to read a different stripe for each row.
	 */
	double maxStripeReadCount = estimatedRows;

	/*
	 * In the best case (i.e the column is fully correlated with the index),
	 * we wouldn't read the same stripe again and again thanks
	 * to locality.
	 */
	double avgStripeRowCount =
		rowCount / (double) ColumnarTableStripeCount(relationId);
	double minStripeReadCount = estimatedRows / avgStripeRowCount;

	/*
	 * While being close to 0 means low correlation, being close to -1 or +1
	 * means high correlation. For index scans on columnar tables, it doesn't
	 * matter if the column and the index are "correlated" (+1) or
	 * "anti-correlated" (-1) since both help us avoiding from reading the
	 * same stripe again and again.
	 */
	double absIndexCorrelation = Abs(indexCorrelation);

	/*
	 * To estimate the number of stripes that we need to read, we do linear
	 * interpolation between minStripeReadCount & maxStripeReadCount. To do
	 * that, we use complement to 1 of absolute correlation, where being
	 * close to 0 means high correlation and being close to 1 means low
	 * correlation.
	 * In practice, we only want to do an index scan when absIndexCorrelation
	 * is 1 (or extremely close to it), or when the absolute number of tuples
	 * returned is very small. Other cases will have a prohibitive cost.
	 */
	double complementIndexCorrelation = 1 - absIndexCorrelation;
	double estimatedStripeReadCount =
		minStripeReadCount + complementIndexCorrelation * (maxStripeReadCount -
														   minStripeReadCount);

	/* even in the best case, we will read a single stripe */
	estimatedStripeReadCount = Max(estimatedStripeReadCount, 1.0);

	Cost scanCost = perStripeCost * estimatedStripeReadCount;

	ereport(DEBUG4, (errmsg("re-costing index scan for columnar table: "
							"selectivity = %.10f, complement abs "
							"correlation = %.10f, per stripe cost = %.10f, "
							"estimated stripe read count = %.10f, "
							"total additional cost = %.10f",
							indexSelectivity, complementIndexCorrelation,
							perStripeCost, estimatedStripeReadCount,
							scanCost)));

	return scanCost;
}

/*
 * RelationIdGetNumberOfAttributes returns number of attributes that relation
 * with relationId has.
 */
static int
RelationIdGetNumberOfAttributes(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	int nattrs = relation->rd_att->natts;
	RelationClose(relation);
	return nattrs;
}

/*
 * ColumnarPerStripeScanCost calculates the cost to scan a single stripe
 * of given columnar table based on number of columns that needs to be
 * read during scan operation.
 */
static Cost
ColumnarPerStripeScanCost(RelOptInfo *rel, Oid relationId, int numberOfColumnsRead)
{
	Relation relation = RelationIdGetRelation(relationId);
	List *stripeList = StripesForRelfilenode(relation->rd_node);
	RelationClose(relation);

	uint32 maxColumnCount = 0;
	uint64 totalStripeSize = 0;
	StripeMetadata *stripeMetadata = NULL;
	foreach_ptr(stripeMetadata, stripeList)
	{
		totalStripeSize += stripeMetadata->dataLength;
		maxColumnCount = Max(maxColumnCount, stripeMetadata->columnCount);
	}

	/*
	 * When no stripes are in the table we don't have a count in maxColumnCount. To
	 * prevent a division by zero turning into a NaN we keep the ratio on zero.
	 * This will result in a cost of 0 for scanning the table which is a reasonable
	 * cost on an empty table.
	 */
	if (maxColumnCount == 0)
	{
		return 0;
	}

	double columnSelectionRatio = numberOfColumnsRead / (double) maxColumnCount;
	Cost tableScanCost = (double) totalStripeSize / BLCKSZ * columnSelectionRatio;
	Cost perStripeScanCost = tableScanCost / list_length(stripeList);

	/*
	 * Finally, multiply the cost of reading a single stripe by seq page read
	 * cost to make our estimation scale compatible with postgres.
	 * Since we are calculating the cost for a single stripe here, we use seq
	 * page cost instead of random page cost. This is because, random page
	 * access only happens when switching between columns, which is pretty
	 * much neglactable.
	 */
	double relSpaceSeqPageCost;
	get_tablespace_page_costs(rel->reltablespace,
							  NULL, &relSpaceSeqPageCost);
	perStripeScanCost = perStripeScanCost * relSpaceSeqPageCost;

	return perStripeScanCost;
}

/*
 * ColumnarTableStripeCount returns the number of stripes that columnar
 * table with relationId has by using stripe metadata.
 */
static uint64
ColumnarTableStripeCount(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	List *stripeList = StripesForRelfilenode(relation->rd_node);
	int stripeCount = list_length(stripeList);
	RelationClose(relation);

	return stripeCount;
}

/*
 * CreateColumnarSeqScanPath returns Path for sequential scan on columnar
 * table with relationId.
 */
static Path *
CreateColumnarSeqScanPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId)
{
	/* columnar doesn't support parallel scan */
	int parallelWorkers = 0;

	Relids requiredOuter = rel->lateral_relids;
	Path *path = create_seqscan_path(root, rel, requiredOuter, parallelWorkers);
	CostColumnarSeqPath(rel, relationId, path);
	return path;
}

/*
 * AddColumnarScanPathsRec is a recursive function to search the
 * parameterization space and add CustomPaths for columnar scans.
 *
 * The set paramRelids is the parameterization at the current level, and
 * candidateRelids is the set from which we draw to generate paths with
 * greater parameterization.
 *
 * Columnar tables resemble indexes because of the ability to push down
 * quals. Ordinary quals, such as x = 7, can be pushed down easily. But join
 * quals of the form "x = y" (where "y" comes from another rel) require the
 * proper parameterization.
 *
 * Paths that require more outer rels can push down more join clauses that
 * depend on those outer rels. But requiring more outer rels gives the planner
 * fewer options for the shape of the plan. That means there is a trade-off,
 * and we should generate plans of various parameterizations, then let the
 * planner choose. We always need to generate one minimally-parameterized path
 * (parameterized only by lateral refs, if present) to make sure that at least
 * one path can be chosen. Then, we generate as many parameterized paths as we
 * reasonably can.
 *
 * The set of all possible parameterizations is the power set of
 * candidateRelids. The power set has cardinality 2^N, where N is the
 * cardinality of candidateRelids. To avoid creating a huge number of paths,
 * limit the depth of the search; the depthLimit is equivalent to the maximum
 * number of required outer rels (beyond the minimal parameterization) for the
 * path. A depthLimit of zero means that only the minimally-parameterized path
 * will be generated.
 */
static void
AddColumnarScanPathsRec(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
						Relids paramRelids, Relids candidateRelids,
						int depthLimit)
{
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	Assert(!bms_overlap(paramRelids, candidateRelids));
	AddColumnarScanPath(root, rel, rte, paramRelids);

	/* recurse for all candidateRelids, unless we hit the depth limit */
	Assert(depthLimit >= 0);
	if (depthLimit-- == 0)
	{
		return;
	}

	/*
	 * Iterate through parameter combinations depth-first. Deeper levels
	 * generate paths of greater parameterization (and hopefully lower
	 * cost).
	 */
	Relids tmpCandidateRelids = bms_copy(candidateRelids);
	int relid = -1;
	while ((relid = bms_next_member(candidateRelids, relid)) >= 0)
	{
		Relids tmpParamRelids = bms_add_member(
			bms_copy(paramRelids), relid);

		/*
		 * Because we are generating combinations (not permutations), remove
		 * the relid from the set of candidates at this level as we descend to
		 * the next.
		 */
		tmpCandidateRelids = bms_del_member(tmpCandidateRelids, relid);

		AddColumnarScanPathsRec(root, rel, rte, tmpParamRelids,
								tmpCandidateRelids, depthLimit);
	}

	bms_free(tmpCandidateRelids);
}


void
columnar_hooks_init()
{
	PreviousSetRelPathlistHook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = ColumnarSetRelPathlistHook;

	PreviousGetRelationInfoHook = get_relation_info_hook;
	get_relation_info_hook = ColumnarGetRelationInfoHook;

	PreviousProcessUtilityHook = ProcessUtility_hook ?
								 ProcessUtility_hook :
								 standard_ProcessUtility;

	ProcessUtility_hook = ColumnarProcessUtility;

	columnar_customscan_register();

	/* Vectorization */

	PreviousPlannerHook = planner_hook;
	planner_hook = ColumnarVectorPlanerHook;

	columnar_vectorscan_register();
	columnar_unbatch_node_register();
	columnar_agg_node_register();
}


void
columnar_hooks_fini()
{
	planner_hook = PreviousPlannerHook;
	set_rel_pathlist_hook = PreviousSetRelPathlistHook;
	get_relation_info_hook = PreviousGetRelationInfoHook;
	ProcessUtility_hook = PreviousProcessUtilityHook;
}