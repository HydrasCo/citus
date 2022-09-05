/*-------------------------------------------------------------------------
 *
 * columnar_hooks.h
 *	  Columnar PostgreSQL Hooks
 * 
 * Copyright (c) Citus Data, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/columnar/columnar_hooks.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_HOOKS_H
#define COLUMNAR_HOOKS_H

extern void columnar_hooks_init(void);
extern void columnar_hooks_fini(void);

#endif