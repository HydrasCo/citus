/*-------------------------------------------------------------------------
 *
 * columnar_custompath.h
 *
 * Forward declearation of functions to hookup custom path.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_CUSTOMPATH_H
#define COLUMNAR_CUSTOMPATH_H

#include "nodes/extensible.h"

extern const CustomPathMethods * columnar_custompath_methods(void);

#endif