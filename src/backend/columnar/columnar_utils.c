/*-------------------------------------------------------------------------
 *
 * columnar_utils.c
 *	Utils needed for various scan methods
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "access/genam.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

#include "columnar/columnar.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_utils.h"
#include "columnar/utils/listutils.h"


/* functions for CheckCitusColumnarVersion */
static bool CheckAvailableVersionColumnar(int elevel);
static bool CheckInstalledVersionColumnar(int elevel);
static char * AvailableExtensionVersionColumnar(void);
static char * InstalledExtensionVersionColumnar(void);
static bool CitusColumnarHasBeenLoadedInternal(void);
static bool CitusColumnarHasBeenLoaded(void);
static bool MajorVersionsCompatibleColumnar(char *leftVersion, char *rightVersion);

/* global variables for CheckCitusColumnarVersion */
static bool extensionLoadedColumnar = false;
static bool citusVersionKnownCompatibleColumnar = false;

/*
 * ReparameterizeMutator changes all varnos referencing the topmost parent of
 * child_rel to instead reference child_rel directly.
 */
Node *
ReparameterizeMutator(Node *node, RelOptInfo *child_rel)
{
	if (node == NULL)
	{
		return NULL;
	}
	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);
		if (bms_is_member(var->varno, child_rel->top_parent_relids))
		{
			var = copyObject(var);
			var->varno = child_rel->relid;
		}
		return (Node *) var;
	}

	if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = castNode(RestrictInfo, node);
		rinfo = copyObject(rinfo);
		rinfo->clause = (Expr *) expression_tree_mutator(
			(Node *) rinfo->clause, ReparameterizeMutator, (void *) child_rel);
		return (Node *) rinfo;
	}
	return expression_tree_mutator(node, ReparameterizeMutator,
								   (void *) child_rel);
}


/*
 * ColumnarAttrNeeded returns a list of AttrNumber's for the ones that are
 * needed during columnar custom scan.
 * Throws an error if finds a Var referencing to an attribute not supported
 * by ColumnarScan.
 */
Bitmapset *
ColumnarAttrNeeded(ScanState *ss)
{
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;
	int natts = slot->tts_tupleDescriptor->natts;
	Bitmapset *attr_needed = NULL;
	Plan *plan = ss->ps.plan;
	int flags = PVC_RECURSE_AGGREGATES |
				PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
	List *vars = list_concat(pull_var_clause((Node *) plan->targetlist, flags),
							 pull_var_clause((Node *) plan->qual, flags));
	ListCell *lc;

	foreach(lc, vars)
	{
		Var *var = lfirst(lc);

		if (var->varattno < 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg(
								"UPDATE and CTID scans not supported for ColumnarScan")));
		}

		if (var->varattno == 0)
		{
			elog(DEBUG1, "Need attribute: all");

			/* all attributes are required, we don't need to add more so break*/
			attr_needed = bms_add_range(attr_needed, 0, natts - 1);
			break;
		}

		elog(DEBUG1, "Need attribute: %d", var->varattno);
		attr_needed = bms_add_member(attr_needed, var->varattno - 1);
	}

	return attr_needed;
}


 /*
 * ColumnarVarNeeded returns a list of Var objects for the ones that are
 * needed during columnar custom scan.
 * Throws an error if finds a Var referencing to an attribute not supported
 * by ColumnarScan.
 */
List *
ColumnarVarNeeded(CustomScanState *customScanState)
{
	ScanState *scanState = &customScanState->ss;

	List *varList = NIL;

	Bitmapset *neededAttrSet = ColumnarAttrNeeded(scanState);
	int bmsMember = -1;
	while ((bmsMember = bms_next_member(neededAttrSet, bmsMember)) >= 0)
	{
		Relation columnarRelation = scanState->ss_currentRelation;

		/* neededAttrSet already represents 0-indexed attribute numbers */
		Form_pg_attribute columnForm =
			TupleDescAttr(RelationGetDescr(columnarRelation), bmsMember);
		if (columnForm->attisdropped)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
							errmsg("cannot explain column with attrNum=%d "
								   "of columnar table %s since it is dropped",
								   bmsMember + 1,
								   RelationGetRelationName(columnarRelation))));
		}
		else if (columnForm->attnum <= 0)
		{
			/*
			 * ColumnarAttrNeeded should have already thrown an error for
			 * system columns. Similarly, it should have already expanded
			 * whole-row references to individual attributes.
			 */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot explain column with attrNum=%d "
								   "of columnar table %s since it is either "
								   "a system column or a whole-row "
								   "reference", columnForm->attnum,
								   RelationGetRelationName(columnarRelation))));
		}


		/*
		 * varlevelsup is used to figure out the (query) level of the Var
		 * that we are investigating. Since we are dealing with a particular
		 * relation, it is useless here.
		 */
		Index varlevelsup = 0;

		CustomScan *customScan = (CustomScan *) customScanState->ss.ps.plan;
		Index scanrelid = customScan->scan.scanrelid;
		Var *var = makeVar(scanrelid, columnForm->attnum, columnForm->atttypid,
						   columnForm->atttypmod, columnForm->attcollation,
						   varlevelsup);
		varList = lappend(varList, var);
	}

	return varList;
}

/*
 * ColumnarPushdownClausesStr represents the clauses to push down as a string.
 */
const char *
ColumnarPushdownClausesStr(List *context, List *clauses)
{
	Expr *conjunction;

	Assert(list_length(clauses) > 0);

	if (list_length(clauses) == 1)
	{
		conjunction = (Expr *) linitial(clauses);
	}
	else
	{
		conjunction = make_andclause(clauses);
	}

	bool useTableNamePrefix = false;
	bool showImplicitCast = false;
	return deparse_expression((Node *) conjunction, context,
							  useTableNamePrefix, showImplicitCast);
}


/*
 * ColumnarProjectedColumnsStr generates projected column string for
 * explain output.
 */
const char *
ColumnarProjectedColumnsStr(List *context, List *projectedColumns)
{
	if (list_length(projectedColumns) == 0)
	{
		return "<columnar optimized out all columns>";
	}

	bool useTableNamePrefix = false;
	bool showImplicitCast = false;
	return deparse_expression((Node *) projectedColumns, context,
							  useTableNamePrefix, showImplicitCast);
}

/*
 * EvalParamsMutator evaluates Params in the expression and replaces them with
 * Consts.
 */
Node *
EvalParamsMutator(Node *node, ExprContext *econtext)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Param))
	{
		Param *param = (Param *) node;
		int16 typLen;
		bool typByVal;
		bool isnull;

		get_typlenbyval(param->paramtype, &typLen, &typByVal);

		/* XXX: should save ExprState for efficiency */
		ExprState *exprState = ExecInitExprWithParams((Expr *) node,
													  econtext->ecxt_param_list_info);
		Datum pval = ExecEvalExpr(exprState, econtext, &isnull);

		return (Node *) makeConst(param->paramtype,
								  param->paramtypmod,
								  param->paramcollid,
								  (int) typLen,
								  pval,
								  isnull,
								  typByVal);
	}

	return expression_tree_mutator(node, EvalParamsMutator, (void *) econtext);
}


/*
 * set_deparse_context_planstate is a compatibility wrapper for versions 13+.
 */
List *
set_deparse_context_planstate(List *dpcontext, Node *node, List *ancestors)
{
	PlanState *ps = (PlanState *) node;
	return set_deparse_context_plan(dpcontext, ps->plan, ancestors);
}


/*
 * Code to check the Citus Version, helps remove dependency from Citus
 */

/*
 * CitusColumnarHasBeenLoaded returns true if the citus extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false. The result is cached as this is called very frequently.
 */
bool
CitusColumnarHasBeenLoaded(void)
{
	if (!extensionLoadedColumnar || creating_extension)
	{
		/*
		 * Refresh if we have not determined whether the extension has been
		 * loaded yet, or in case of ALTER EXTENSION since we want to treat
		 * Citus as "not loaded" during ALTER EXTENSION citus.
		 */
		bool extensionLoaded = CitusColumnarHasBeenLoadedInternal();
		extensionLoadedColumnar = extensionLoaded;
	}

	return extensionLoadedColumnar;
}


/*
 * CitusColumnarHasBeenLoadedInternal returns true if the citus extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false.
 */
static bool
CitusColumnarHasBeenLoadedInternal(void)
{
	if (IsBinaryUpgrade)
	{
		/* never use Citus logic during pg_upgrade */
		return false;
	}

	Oid citusExtensionOid = get_extension_oid("citus", true);
	if (citusExtensionOid == InvalidOid)
	{
		/* Citus extension does not exist yet */
		return false;
	}

	if (creating_extension && CurrentExtensionObject == citusExtensionOid)
	{
		/*
		 * We do not use Citus hooks during CREATE/ALTER EXTENSION citus
		 * since the objects used by the C code might be not be there yet.
		 */
		return false;
	}

	/* citus extension exists and has been created */
	return true;
}


/*
 * CheckCitusColumnarVersion checks whether there is a version mismatch between the
 * available version and the loaded version or between the installed version
 * and the loaded version. Returns true if compatible, false otherwise.
 *
 * As a side effect, this function also sets citusVersionKnownCompatible_Columnar global
 * variable to true which reduces version check cost of next calls.
 */
bool
CheckCitusColumnarVersion(int elevel)
{
	if (citusVersionKnownCompatibleColumnar ||
		!CitusColumnarHasBeenLoaded() ||
		!columnar_enable_version_checks)
	{
		return true;
	}

	if (CheckAvailableVersionColumnar(elevel) && CheckInstalledVersionColumnar(elevel))
	{
		citusVersionKnownCompatibleColumnar = true;
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * CheckAvailableVersion compares CITUS_EXTENSIONVERSION and the currently
 * available version from the citus.control file. If they are not compatible,
 * this function logs an error with the specified elevel and returns false,
 * otherwise it returns true.
 */
bool
CheckAvailableVersionColumnar(int elevel)
{
	if (!columnar_enable_version_checks)
	{
		return true;
	}

	char *availableVersion = AvailableExtensionVersionColumnar();

	if (!MajorVersionsCompatibleColumnar(availableVersion, CITUS_EXTENSIONVERSION))
	{
		ereport(elevel, (errmsg("loaded Citus library version differs from latest "
								"available extension version"),
						 errdetail("Loaded library requires %s, but the latest control "
								   "file specifies %s.", CITUS_MAJORVERSION,
								   availableVersion),
						 errhint("Restart the database to load the latest Citus "
								 "library.")));
		pfree(availableVersion);
		return false;
	}
	pfree(availableVersion);
	return true;
}


/*
 * CheckInstalledVersion compares CITUS_EXTENSIONVERSION and the
 * extension's current version from the pg_extension catalog table. If they
 * are not compatible, this function logs an error with the specified elevel,
 * otherwise it returns true.
 */
static bool
CheckInstalledVersionColumnar(int elevel)
{
	Assert(CitusColumnarHasBeenLoaded());
	Assert(columnar_enable_version_checks);

	char *installedVersion = InstalledExtensionVersionColumnar();

	if (!MajorVersionsCompatibleColumnar(installedVersion, CITUS_EXTENSIONVERSION))
	{
		ereport(elevel, (errmsg("loaded Citus library version differs from installed "
								"extension version"),
						 errdetail("Loaded library requires %s, but the installed "
								   "extension version is %s.", CITUS_MAJORVERSION,
								   installedVersion),
						 errhint("Run ALTER EXTENSION citus UPDATE and try again.")));
		pfree(installedVersion);
		return false;
	}
	pfree(installedVersion);
	return true;
}


/*
 * MajorVersionsCompatible checks whether both versions are compatible. They
 * are if major and minor version numbers match, the schema version is
 * ignored.  Returns true if compatible, false otherwise.
 */
bool
MajorVersionsCompatibleColumnar(char *leftVersion, char *rightVersion)
{
	const char schemaVersionSeparator = '-';

	char *leftSeperatorPosition = strchr(leftVersion, schemaVersionSeparator);
	char *rightSeperatorPosition = strchr(rightVersion, schemaVersionSeparator);
	int leftComparisionLimit = 0;
	int rightComparisionLimit = 0;

	if (leftSeperatorPosition != NULL)
	{
		leftComparisionLimit = leftSeperatorPosition - leftVersion;
	}
	else
	{
		leftComparisionLimit = strlen(leftVersion);
	}

	if (rightSeperatorPosition != NULL)
	{
		rightComparisionLimit = rightSeperatorPosition - rightVersion;
	}
	else
	{
		rightComparisionLimit = strlen(leftVersion);
	}

	/* we can error out early if hypens are not in the same position */
	if (leftComparisionLimit != rightComparisionLimit)
	{
		return false;
	}

	return strncmp(leftVersion, rightVersion, leftComparisionLimit) == 0;
}


/*
 * AvailableExtensionVersion returns the Citus version from citus.control file. It also
 * saves the result, thus consecutive calls to CitusExtensionAvailableVersion will
 * not read the citus.control file again.
 */
static char *
AvailableExtensionVersionColumnar(void)
{
	LOCAL_FCINFO(fcinfo, 0);
	FmgrInfo flinfo;

	bool goForward = true;
	bool doCopy = false;
	char *availableExtensionVersion;

	EState *estate = CreateExecutorState();
	ReturnSetInfo *extensionsResultSet = makeNode(ReturnSetInfo);
	extensionsResultSet->econtext = GetPerTupleExprContext(estate);
	extensionsResultSet->allowedModes = SFRM_Materialize;

	fmgr_info(F_PG_AVAILABLE_EXTENSIONS, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 0, InvalidOid, NULL,
							 (Node *) extensionsResultSet);

	/* pg_available_extensions returns result set containing all available extensions */
	(*pg_available_extensions)(fcinfo);

	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlotCompat(
		extensionsResultSet->setDesc,
		&TTSOpsMinimalTuple);
	bool hasTuple = tuplestore_gettupleslot(extensionsResultSet->setResult, goForward,
											doCopy,
											tupleTableSlot);
	while (hasTuple)
	{
		bool isNull = false;

		Datum extensionNameDatum = slot_getattr(tupleTableSlot, 1, &isNull);
		char *extensionName = NameStr(*DatumGetName(extensionNameDatum));
		if (strcmp(extensionName, "citus") == 0)
		{
			Datum availableVersion = slot_getattr(tupleTableSlot, 2, &isNull);


			availableExtensionVersion = text_to_cstring(DatumGetTextPP(availableVersion));


			ExecClearTuple(tupleTableSlot);
			ExecDropSingleTupleTableSlot(tupleTableSlot);

			return availableExtensionVersion;
		}

		ExecClearTuple(tupleTableSlot);
		hasTuple = tuplestore_gettupleslot(extensionsResultSet->setResult, goForward,
										   doCopy, tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("citus extension is not found")));
}


/*
 * InstalledExtensionVersion returns the Citus version in PostgreSQL pg_extension table.
 */
static char *
InstalledExtensionVersionColumnar(void)
{
	ScanKeyData entry[1];
	char *installedExtensionVersion = NULL;

	Relation relation = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0], Anum_pg_extension_extname, BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("citus"));

	SysScanDesc scandesc = systable_beginscan(relation, ExtensionNameIndexId, true,
											  NULL, 1, entry);

	HeapTuple extensionTuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(extensionTuple))
	{
		int extensionIndex = Anum_pg_extension_extversion;
		TupleDesc tupleDescriptor = RelationGetDescr(relation);
		bool isNull = false;

		Datum installedVersion = heap_getattr(extensionTuple, extensionIndex,
											  tupleDescriptor, &isNull);

		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("citus extension version is null")));
		}


		installedExtensionVersion = text_to_cstring(DatumGetTextPP(installedVersion));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("citus extension is not loaded")));
	}

	systable_endscan(scandesc);

	table_close(relation, AccessShareLock);

	return installedExtensionVersion;
}