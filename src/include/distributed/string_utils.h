/*-------------------------------------------------------------------------
 *
 * string_utils.h
 *   Utilities related to strings.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_STRING_UTILS_H
#define CITUS_STRING_UTILS_H

#include "postgres.h"

extern char * ConvertIntToString(int val);
extern char* ConcatenateStringListWithDelimiter(List* stringList, char delimiter);

#endif /* CITUS_STRING_UTILS_H */
