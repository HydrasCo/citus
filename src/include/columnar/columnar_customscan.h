/*-------------------------------------------------------------------------
 *
 * columnar_customscan.h
 *
 * Forward declarations of functions to hookup the custom scan feature of
 * columnar.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_CUSTOMSCAN_H
#define COLUMNAR_CUSTOMSCAN_H

#include "nodes/extensible.h"

extern const CustomScanMethods * columnar_customscan_methods(void);
extern void columnar_customscan_register(void);

#endif /* COLUMNAR_CUSTOMSCAN_H */
