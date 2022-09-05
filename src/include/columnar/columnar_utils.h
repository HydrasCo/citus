/*-------------------------------------------------------------------------
 *
 * columnar_utils.h
 * 
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/columnar/columnar_utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_UTILS_H
#define COLUMNAR_UTILS_H

#include "postgres.h"

#include "nodes/extensible.h"

extern Node * ReparameterizeMutator(Node *node, RelOptInfo *child_rel);
extern Bitmapset * ColumnarAttrNeeded(ScanState *ss);
extern List * ColumnarVarNeeded(CustomScanState *customScanState);
extern const char * ColumnarPushdownClausesStr(List *context, List *clauses);
extern const char * ColumnarProjectedColumnsStr(List *context, List *projectedColumns);
extern Node * EvalParamsMutator(Node *node, ExprContext *econtext);
extern List * set_deparse_context_planstate(List *dpcontext, Node *node, List *ancestors);
extern bool CheckCitusColumnarVersion(int elevel);

#endif
